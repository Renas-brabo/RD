from __future__ import annotations

import logging
import re
import unicodedata
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

from rd_data_multiarquivo.config import get_config
from rd_data_multiarquivo.logging_utils import setup_logger
from rd_data_multiarquivo.validators import validate_config
from rd_data_multiarquivo.collectors import collect_data
from rd_data_multiarquivo.processors import process_data
from rd_data_multiarquivo.exporters import (
    build_export_tables,
    export_to_excel,
    build_execution_summary,
    log_execution_summary,
)
from rd_data_multiarquivo.naming import standardize_column_names


# =========================
# Logging para interface
# =========================
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        msg = self.format(record)
        self.messages.append(msg)


# =========================
# Utilidades de texto / nome
# =========================
def normalize_text(text: str) -> str:
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def month_name_to_number(token: str) -> int | None:
    token = normalize_text(token)

    month_map = {
        "jan": 1,
        "janeiro": 1,
        "fev": 2,
        "fevereiro": 2,
        "mar": 3,
        "marco": 3,
        "abr": 4,
        "abril": 4,
        "mai": 5,
        "maio": 5,
        "jun": 6,
        "junho": 6,
        "jul": 7,
        "julho": 7,
        "ago": 8,
        "agosto": 8,
        "set": 9,
        "setembro": 9,
        "out": 10,
        "outubro": 10,
        "nov": 11,
        "novembro": 11,
        "dez": 12,
        "dezembro": 12,
    }

    return month_map.get(token)


def is_excel_temp_file(path: Path) -> bool:
    name = path.name
    return name.startswith("~$")


def is_hidden_file(path: Path) -> bool:
    return path.name.startswith(".")


def looks_like_rmd_file(path: Path) -> bool:
    """
    Critério amplo, mas seguro, para reconhecer candidatos a RMD.
    """
    if not path.is_file():
        return False
    if path.suffix.lower() != ".xlsx":
        return False
    if is_excel_temp_file(path):
        return False
    if is_hidden_file(path):
        return False

    name = normalize_text(path.stem)

    # Aceita variações comuns
    keywords = ["rmd", "anexo_rmd", "anexo-rmd", "anexo rmd", "divida", "dpf"]
    return any(k in name for k in keywords)


def parse_rmd_month_year_from_name(file_path: Path) -> tuple[int, int] | None:
    """
    Tenta extrair (ano, mês) do nome do arquivo.

    Exemplos aceitos:
      - Anexo_RMD_Janeiro_26.xlsx
      - Anexo-RMD-Fev-2026.xlsx
      - RMD mar 25.xlsx
      - rmd_abril_2024_final.xlsx
      - anexo.rmd.dez.2023.xlsx
    """
    stem = normalize_text(file_path.stem)

    tokens = [t for t in re.split(r"[_\-\s\.]+", stem) if t]

    month_num = None
    year_num = None

    for token in tokens:
        if month_num is None:
            maybe_month = month_name_to_number(token)
            if maybe_month is not None:
                month_num = maybe_month
                continue

        if year_num is None and re.fullmatch(r"\d{2}|\d{4}", token):
            y = int(token)
            year_num = 2000 + y if y < 100 else y

    if month_num is not None and year_num is not None:
        return year_num, month_num

    month_regex = (
        r"(jan(?:eiro)?|fev(?:ereiro)?|mar(?:co)?|abr(?:il)?|mai(?:o)?|"
        r"jun(?:ho)?|jul(?:ho)?|ago(?:sto)?|set(?:embro)?|out(?:ubro)?|"
        r"nov(?:embro)?|dez(?:embro)?)"
    )
    year_regex = r"(\d{2}|\d{4})"

    match = re.search(month_regex + r".*?" + year_regex, stem)
    if not match:
        match = re.search(year_regex + r".*?" + month_regex, stem)

    if match:
        parts = match.groups()
        month_token = None
        year_token = None

        for part in parts:
            if re.fullmatch(r"\d{2}|\d{4}", part):
                year_token = part
            else:
                month_token = part

        if month_token and year_token:
            m = month_name_to_number(month_token)
            y = int(year_token)
            y = 2000 + y if y < 100 else y
            if m is not None:
                return y, m

    return None


def discover_rmd_candidates(rmd_dir: Path) -> list[Path]:
    """
    Procura recursivamente candidatos a RMD dentro da pasta.
    """
    if not rmd_dir.exists():
        raise FileNotFoundError(f"Pasta de RMD não encontrada: {rmd_dir}")

    files = [p for p in rmd_dir.rglob("*.xlsx") if looks_like_rmd_file(p)]
    return sorted(files)


def build_rmd_rank(path: Path) -> tuple:
    """
    Monta um rank robusto para escolher o arquivo "mais recente".

    Prioridade:
    1) arquivo com mês/ano inferíveis do nome;
    2) maior ano;
    3) maior mês;
    4) maior data de modificação;
    5) nome (desempate estável).
    """
    parsed = parse_rmd_month_year_from_name(path)
    mtime = path.stat().st_mtime
    normalized_name = normalize_text(path.name)

    if parsed is not None:
        year_num, month_num = parsed
        return (2, year_num, month_num, mtime, normalized_name)

    return (1, 0, 0, mtime, normalized_name)


def find_latest_rmd_file(rmd_dir: str | Path = "rmd") -> Path:
    """
    Detecta o RMD mais recente de forma blindada.
    """
    rmd_dir = Path(rmd_dir)

    candidates = discover_rmd_candidates(rmd_dir)
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo RMD válido (.xlsx) foi encontrado em: {rmd_dir}"
        )

    ranked = [(build_rmd_rank(p), p) for p in candidates]
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


def current_rmd_signature(path: Path) -> str:
    """
    Assinatura do arquivo para detectar mudança automática e rerodar.
    """
    stat = path.stat()
    return f"{path.resolve()}|{int(stat.st_mtime)}|{stat.st_size}"


def get_rmd_search_dir_from_config(cfg: dict) -> Path:
    """
    Deriva a pasta-base de busca a partir do ARQUIVO_RMD configurado.
    Se o config vier como 'rmd/arquivo.xlsx', usa 'rmd/'.
    Se vier vazio ou inválido, usa 'rmd/'.
    """
    configured = str(cfg.get("ARQUIVO_RMD", "")).strip()

    if not configured:
        return Path("rmd")

    p = Path(configured)

    # Se apontar para arquivo, buscamos na pasta pai
    if p.suffix:
        parent = p.parent
        return parent if str(parent) not in ("", ".") else Path("rmd")

    # Se apontar para diretório
    return p


# =========================
# Preview das abas
# =========================
def prepare_preview_df(df: pd.DataFrame, max_rows: int = 50) -> pd.DataFrame:
    """
    Mostra automaticamente as linhas mais recentes.
    """
    df_view = df.copy()

    date_col = None
    year_col = None

    for candidate in ["data", "Data"]:
        if candidate in df_view.columns:
            date_col = candidate
            break

    for candidate in ["ano", "Ano"]:
        if candidate in df_view.columns:
            year_col = candidate
            break

    if date_col:
        df_view[date_col] = pd.to_datetime(df_view[date_col], errors="coerce")
        df_view = (
            df_view.sort_values(date_col, ascending=False)
            .head(max_rows)
            .reset_index(drop=True)
        )
    elif year_col:
        df_view[year_col] = pd.to_numeric(df_view[year_col], errors="coerce")
        df_view = (
            df_view.sort_values(year_col, ascending=False)
            .head(max_rows)
            .reset_index(drop=True)
        )
    else:
        df_view = df_view.tail(max_rows).reset_index(drop=True)

    return df_view


# =========================
# Execução automática
# =========================
def run_pipeline_auto(latest_rmd: Path):
    """
    Executa o pipeline automaticamente usando o RMD detectado.
    """
    cfg = get_config()
    cfg["ARQUIVO_RMD"] = str(latest_rmd)
    cfg["LOG_TO_CONSOLE"] = False
    cfg["LOG_TO_FILE"] = True

    logger, log_artifacts = setup_logger(cfg)

    st_handler = StreamlitLogHandler()
    st_handler.setLevel(logging.INFO)
    st_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(st_handler)

    started_at = datetime.now().timestamp()

    try:
        logger.info("Iniciando execução automática pelo Streamlit.")
        logger.info("Arquivo RMD detectado automaticamente: %s", cfg["ARQUIVO_RMD"])

        validate_config(cfg)

        raw = collect_data(cfg, logger)
        processed, warnings = process_data(raw, cfg, logger)

        export_tables = build_export_tables(processed, logger)
        export_tables = standardize_column_names(export_tables, logger)

        output_path = export_to_excel(export_tables, cfg["OUTPUT_NAME"], logger)

        summary = build_execution_summary(
            export_tables=export_tables,
            warnings=warnings,
            output=output_path,
            logs=log_artifacts,
            started_at=started_at,
        )
        log_execution_summary(logger, summary, warnings)

        with open(output_path, "rb") as f:
            excel_bytes = f.read()

        return {
            "success": True,
            "detected_rmd": str(latest_rmd),
            "detected_rmd_signature": current_rmd_signature(latest_rmd),
            "export_tables": export_tables,
            "warnings": warnings,
            "summary": summary,
            "logs": st_handler.messages,
            "excel_bytes": excel_bytes,
            "output_path": str(output_path),
        }

    except Exception as exc:
        logger.exception("Falha na execução automática: %s", exc)
        return {
            "success": False,
            "detected_rmd": str(latest_rmd),
            "detected_rmd_signature": current_rmd_signature(latest_rmd),
            "error": str(exc),
            "logs": st_handler.messages,
        }


# =========================
# Interface
# =========================
st.set_page_config(
    page_title="RD Data Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 RD Data Dashboard")
st.caption(
    "Execução automática do pipeline com detecção blindada do RMD mais recente."
)

default_cfg = get_config()
rmd_search_dir = get_rmd_search_dir_from_config(default_cfg)

try:
    latest_rmd = find_latest_rmd_file(rmd_search_dir)
    latest_signature = current_rmd_signature(latest_rmd)

    should_run = (
        "rd_result" not in st.session_state
        or st.session_state.get("rd_result", {}).get("detected_rmd_signature") != latest_signature
    )

    if should_run:
        with st.spinner("Localizando o RMD mais recente e executando o pipeline..."):
            st.session_state["rd_result"] = run_pipeline_auto(latest_rmd)

    result = st.session_state.get("rd_result")

    if result:
        if result["success"]:
            st.success("Pipeline executado com sucesso.")
            st.info(f"Arquivo RMD detectado automaticamente: {result['detected_rmd']}")

            st.subheader("Resumo da execução")
            summary = result["summary"]

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Abas exportadas", summary.get("sheet_count", 0))
            col2.metric("Linhas exportadas", summary.get("total_rows_exported", 0))
            col3.metric("Colunas exportadas", summary.get("total_columns_exported", 0))
            col4.metric("Avisos", summary.get("warning_count", 0))

            with st.expander("Detalhes do resumo", expanded=False):
                st.json(summary)

            st.download_button(
                label="📥 Baixar Excel gerado",
                data=result["excel_bytes"],
                file_name=Path(summary["output_excel"]).name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )

            if result["warnings"]:
                st.subheader("Avisos de validação")
                for w in result["warnings"]:
                    st.warning(w)

            with st.expander("Logs da execução", expanded=False):
                for msg in result["logs"]:
                    st.text(msg)

            st.subheader("Pré-visualização das abas")
            st.caption("Exibindo automaticamente até 50 linhas mais recentes por aba.")

            export_tables = result["export_tables"]
            tab_names = list(export_tables.keys())
            tabs = st.tabs(tab_names)

            for tab, sheet_name in zip(tabs, tab_names):
                with tab:
                    df = export_tables[sheet_name]
                    df_view = prepare_preview_df(df, max_rows=50)

                    st.write(f"**Aba:** {sheet_name}")
                    st.write(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")
                    st.dataframe(df_view, use_container_width=True)

                    csv_bytes = df.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        label=f"Baixar CSV da aba {sheet_name}",
                        data=csv_bytes,
                        file_name=f"{sheet_name}.csv",
                        mime="text/csv",
                        key=f"csv_{sheet_name}",
                    )

        else:
            st.error("A execução automática falhou.")
            st.code(result["error"])

            with st.expander("Logs da execução", expanded=True):
                for msg in result["logs"]:
                    st.text(msg)

except FileNotFoundError as exc:
    st.error("Não foi possível localizar um arquivo RMD válido.")
    st.info(
        f"Verifique se existe pelo menos um arquivo .xlsx compatível dentro da pasta '{rmd_search_dir}'."
    )
    st.code(str(exc))

except Exception as exc:
    st.error("Ocorreu um erro inesperado na inicialização do dashboard.")
    st.code(str(exc))
