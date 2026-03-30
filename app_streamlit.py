from __future__ import annotations

import logging
import re
import shutil
import tempfile
import unicodedata
import zipfile
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

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


# =========================================================
# Logging para interface
# =========================================================
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        msg = self.format(record)
        self.messages.append(msg)


# =========================================================
# Utilidades gerais
# =========================================================
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


def current_file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{path.resolve()}|{int(stat.st_mtime)}|{stat.st_size}"


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


# =========================================================
# Descoberta LOCAL do RMD
# =========================================================
def is_excel_temp_file(path: Path) -> bool:
    return path.name.startswith("~$")


def is_hidden_file(path: Path) -> bool:
    return path.name.startswith(".")


def looks_like_rmd_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() != ".xlsx":
        return False
    if is_excel_temp_file(path):
        return False
    if is_hidden_file(path):
        return False

    name = normalize_text(path.stem)
    keywords = ["rmd", "anexo_rmd", "anexo-rmd", "anexo rmd", "divida", "dpf"]
    return any(k in name for k in keywords)


def parse_rmd_month_year_from_name(file_path: Path) -> tuple[int, int] | None:
    """
    Tenta extrair (ano, mês) do nome do arquivo.
    Exemplos:
      - Anexo_RMD_Janeiro_26.xlsx
      - Anexo-RMD-Fev-2026.xlsx
      - RMD mar 25.xlsx
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


def build_local_rmd_rank(path: Path) -> tuple:
    parsed = parse_rmd_month_year_from_name(path)
    mtime = path.stat().st_mtime
    normalized_name = normalize_text(path.name)

    if parsed is not None:
        year_num, month_num = parsed
        return (2, year_num, month_num, mtime, normalized_name)

    return (1, 0, 0, mtime, normalized_name)


def get_rmd_search_dir_from_config(cfg: dict) -> Path:
    configured = str(cfg.get("ARQUIVO_RMD", "")).strip()

    if not configured:
        return Path("rmd")

    p = Path(configured)

    if p.suffix:
        parent = p.parent
        return parent if str(parent) not in ("", ".") else Path("rmd")

    return p


def find_latest_local_rmd_file(rmd_dir: str | Path = "rmd") -> Path:
    rmd_dir = Path(rmd_dir)

    if not rmd_dir.exists():
        raise FileNotFoundError(f"Pasta de RMD não encontrada: {rmd_dir}")

    candidates = [p for p in rmd_dir.rglob("*.xlsx") if looks_like_rmd_file(p)]
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo RMD válido (.xlsx) foi encontrado em: {rmd_dir}"
        )

    ranked = [(build_local_rmd_rank(p), p) for p in candidates]
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


# =========================================================
# Descoberta WEB do RMD
# =========================================================
def build_rmd_page_url(year: int, month: int) -> str:
    return (
        "https://www.tesourotransparente.gov.br/publicacoes/"
        f"relatorio-mensal-da-divida-rmd/{year}/{month}"
    )


def iter_recent_year_months(max_lookback_months: int = 18):
    """
    Gera pares (ano, mês) do mês atual para trás.
    """
    today = date.today()
    y, m = today.year, today.month

    for _ in range(max_lookback_months):
        yield y, m
        m -= 1
        if m == 0:
            m = 12
            y -= 1


def fetch_html(url: str, timeout: int = 60) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def score_attachment_candidate(full_url: str, text: str) -> int:
    href_low = full_url.lower()
    text_low = normalize_text(text)

    score = 0

    if href_low.endswith(".xlsx"):
        score += 5
    elif href_low.endswith(".zip"):
        score += 4

    if "anexo" in href_low or "anexo" in text_low:
        score += 3
    if "rmd" in href_low or "rmd" in text_low:
        score += 3
    if "tabela" in href_low or "tabela" in text_low:
        score += 2
    if "anex" in href_low or "anex" in text_low:
        score += 1

    return score


def find_rmd_attachment_in_page(page_url: str) -> dict:
    """
    Retorna o melhor candidato de anexo .zip/.xlsx na página do RMD.
    """
    html = fetch_html(page_url, timeout=60)
    soup = BeautifulSoup(html, "html.parser")

    candidates = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        text = a.get_text(" ", strip=True)
        full_url = urljoin(page_url, href)

        href_low = full_url.lower()
        if href_low.endswith(".zip") or href_low.endswith(".xlsx"):
            score = score_attachment_candidate(full_url, text)
            candidates.append(
                {
                    "score": score,
                    "attachment_url": full_url,
                    "anchor_text": text,
                }
            )

    if not candidates:
        raise FileNotFoundError(
            f"Não encontrei anexo .zip/.xlsx na página do RMD: {page_url}"
        )

    candidates.sort(key=lambda x: x["score"], reverse=True)
    best = candidates[0]
    return {
        "page_url": page_url,
        "attachment_url": best["attachment_url"],
        "anchor_text": best["anchor_text"],
    }


def discover_latest_rmd_on_web(max_lookback_months: int = 18) -> dict:
    """
    Procura o RMD mais recente na web, testando mês atual e retrocedendo.
    """
    errors = []

    for year, month in iter_recent_year_months(max_lookback_months=max_lookback_months):
        page_url = build_rmd_page_url(year, month)

        try:
            found = find_rmd_attachment_in_page(page_url)
            return {
                "source_type": "web",
                "source_label": "Portal Tesouro Transparente",
                "source_signature": f"web|{found['page_url']}|{found['attachment_url']}",
                "page_url": found["page_url"],
                "attachment_url": found["attachment_url"],
                "anchor_text": found["anchor_text"],
                "reference_year": year,
                "reference_month": month,
            }
        except Exception as exc:
            errors.append(f"{page_url} -> {exc}")

    raise FileNotFoundError(
        "Não foi possível localizar um anexo de RMD na web dentro da janela de busca."
    )


def download_file_to_temp(url: str, suffix: str | None = None) -> str:
    headers = {"User-Agent": "Mozilla/5.0"}
    resp = requests.get(url, headers=headers, timeout=120)
    resp.raise_for_status()

    if suffix is None:
        m = re.search(r"(\.zip|\.xlsx)(?:\?|$)", url.lower())
        suffix = m.group(1) if m else ".bin"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(resp.content)
        return tmp.name


def extract_excel_from_zip(zip_path: str) -> tuple[str, str]:
    """
    Extrai o primeiro .xlsx relevante do ZIP.
    Retorna:
      (excel_path, temp_extract_dir)
    """
    extract_dir = tempfile.mkdtemp(prefix="rmd_zip_")

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    excel_files = [p for p in Path(extract_dir).rglob("*.xlsx") if p.is_file()]
    if not excel_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .xlsx foi encontrado dentro do ZIP: {zip_path}"
        )

    def rank_excel_inside_zip(path: Path):
        name = normalize_text(path.name)
        return (
            "rmd" in name,
            "anexo" in name,
            "tabela" in name,
            name,
        )

    excel_files.sort(key=rank_excel_inside_zip, reverse=True)
    return str(excel_files[0]), extract_dir


def materialize_rmd_excel(source_info: dict) -> tuple[str, list[str], list[str]]:
    """
    Converte a origem escolhida em um caminho local de Excel pronto para o pipeline.
    Retorna:
      excel_path, temp_files, temp_dirs
    """
    temp_files = []
    temp_dirs = []

    if source_info["source_type"] == "local":
        return source_info["local_path"], temp_files, temp_dirs

    attachment_url = source_info["attachment_url"]
    lower = attachment_url.lower()

    downloaded_path = download_file_to_temp(
        attachment_url,
        suffix=".zip" if ".zip" in lower else ".xlsx",
    )
    temp_files.append(downloaded_path)

    if downloaded_path.lower().endswith(".xlsx"):
        return downloaded_path, temp_files, temp_dirs

    if downloaded_path.lower().endswith(".zip"):
        excel_path, extract_dir = extract_excel_from_zip(downloaded_path)
        temp_dirs.append(extract_dir)
        return excel_path, temp_files, temp_dirs

    raise ValueError(f"Formato de arquivo inesperado: {downloaded_path}")


def discover_preferred_rmd_source(cfg: dict) -> dict:
    """
    Estratégia:
    1) tenta web;
    2) se falhar, usa o RMD local mais recente da pasta configurada.
    """
    local_dir = get_rmd_search_dir_from_config(cfg)

    try:
        return discover_latest_rmd_on_web(max_lookback_months=18)
    except Exception as web_exc:
        latest_local = find_latest_local_rmd_file(local_dir)
        return {
            "source_type": "local",
            "source_label": "Repositório local",
            "source_signature": f"local|{current_file_signature(latest_local)}",
            "local_path": str(latest_local),
            "fallback_reason": str(web_exc),
        }


# =========================================================
# Execução do pipeline
# =========================================================
def run_pipeline_auto(source_info: dict):
    cfg = get_config()
    temp_files = []
    temp_dirs = []

    try:
        excel_path, temp_files, temp_dirs = materialize_rmd_excel(source_info)

        cfg["ARQUIVO_RMD"] = excel_path
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

        logger.info("Iniciando execução automática pelo Streamlit.")
        logger.info("Fonte RMD selecionada: %s", source_info["source_label"])
        logger.info("ARQUIVO_RMD em uso: %s", cfg["ARQUIVO_RMD"])

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
            "source_info": source_info,
            "source_signature": source_info["source_signature"],
            "export_tables": export_tables,
            "warnings": warnings,
            "summary": summary,
            "logs": st_handler.messages,
            "excel_bytes": excel_bytes,
            "output_path": str(output_path),
        }

    except Exception as exc:
        return {
            "success": False,
            "source_info": source_info,
            "source_signature": source_info.get("source_signature"),
            "error": str(exc),
            "logs": [],
        }

    finally:
        # limpa temporários baixados da web
        for f in temp_files:
            try:
                if f and Path(f).exists():
                    Path(f).unlink()
            except Exception:
                pass

        for d in temp_dirs:
            try:
                if d and Path(d).exists():
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


# =========================================================
# Interface
# =========================================================
st.set_page_config(
    page_title="RD Data Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 RD Data Dashboard")
st.caption(
    "Execução automática do pipeline com busca web do RMD e fallback local."
)

default_cfg = get_config()

try:
    latest_source = discover_preferred_rmd_source(default_cfg)
    latest_signature = latest_source["source_signature"]

    should_run = (
        "rd_result" not in st.session_state
        or st.session_state.get("rd_result", {}).get("source_signature") != latest_signature
    )

    if should_run:
        with st.spinner("Localizando a fonte mais recente do RMD e executando o pipeline..."):
            st.session_state["rd_result"] = run_pipeline_auto(latest_source)

    result = st.session_state.get("rd_result")

    if result:
        source_info = result.get("source_info", {})

        if result["success"]:
            st.success("Pipeline executado com sucesso.")

            st.subheader("Fonte do RMD utilizada")
            st.write(f"**Origem:** {source_info.get('source_label', '-')}")
            st.write(f"**Tipo:** {source_info.get('source_type', '-')}")

            if source_info.get("source_type") == "web":
                st.markdown(
                    f"**Página do RMD:** [{source_info['page_url']}]({source_info['page_url']})"
                )
                st.markdown(
                    f"**Anexo localizado:** [{source_info['attachment_url']}]({source_info['attachment_url']})"
                )
                if source_info.get("anchor_text"):
                    st.write(f"**Texto do link do anexo:** {source_info['anchor_text']}")
            else:
                st.write(f"**Arquivo local:** {source_info.get('local_path', '-')}")
                if source_info.get("fallback_reason"):
                    with st.expander("Motivo do fallback local", expanded=False):
                        st.code(source_info["fallback_reason"])

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
            st.code(result.get("error", "Erro não detalhado."))

except Exception as exc:
    st.error("Não foi possível inicializar o dashboard.")
    st.code(str(exc))
