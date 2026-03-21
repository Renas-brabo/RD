from __future__ import annotations

import os
import tempfile
import logging
from pathlib import Path
from datetime import datetime

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


class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        msg = self.format(record)
        self.messages.append(msg)


def prepare_preview_df(df: pd.DataFrame, mode: str, max_rows: int) -> pd.DataFrame:
    """
    Prepara o DataFrame para preview no Streamlit.
    mode:
      - 'recentes'
      - 'antigas'
      - 'completo'
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
        if mode == "recentes":
            df_view = df_view.sort_values(date_col, ascending=False).head(max_rows)
        elif mode == "antigas":
            df_view = df_view.sort_values(date_col, ascending=True).head(max_rows)
        elif mode == "completo":
            df_view = df_view.sort_values(date_col, ascending=False)

    elif year_col:
        df_view[year_col] = pd.to_numeric(df_view[year_col], errors="coerce")
        if mode == "recentes":
            df_view = df_view.sort_values(year_col, ascending=False).head(max_rows)
        elif mode == "antigas":
            df_view = df_view.sort_values(year_col, ascending=True).head(max_rows)
        elif mode == "completo":
            df_view = df_view.sort_values(year_col, ascending=False)

    else:
        if mode == "recentes":
            df_view = df_view.tail(max_rows)
        elif mode == "antigas":
            df_view = df_view.head(max_rows)
        elif mode == "completo":
            df_view = df_view.copy()

    return df_view.reset_index(drop=True)


def run_pipeline(
    rmd_uploaded_file,
    output_name: str,
    start_sgs: str,
    days_daily_sgs: int,
    mes_alvo: str,
    ano_inicio_rmd: int,
    aba_rmd: str,
    rmd_extraction_mode: str,
):
    cfg = get_config()

    cfg["OUTPUT_NAME"] = output_name
    cfg["START_SGS"] = start_sgs
    cfg["DAYS_DAILY_SGS"] = int(days_daily_sgs)
    cfg["MES_ALVO"] = mes_alvo
    cfg["ANO_INICIO_RMD"] = int(ano_inicio_rmd)
    cfg["ABA_RMD"] = aba_rmd
    cfg["RMD_EXTRACTION_MODE"] = rmd_extraction_mode

    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(rmd_uploaded_file.getbuffer())
        tmp_path = tmp.name

    cfg["ARQUIVO_RMD"] = tmp_path
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
        logger.info("Iniciando execução pelo Streamlit.")
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

        result = {
            "success": True,
            "export_tables": export_tables,
            "warnings": warnings,
            "summary": summary,
            "logs": st_handler.messages,
            "excel_bytes": excel_bytes,
            "output_path": str(output_path),
        }

    except Exception as exc:
        logger.exception("Falha na execução: %s", exc)
        result = {
            "success": False,
            "error": str(exc),
            "logs": st_handler.messages,
        }

    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except Exception:
                pass

    return result


st.set_page_config(
    page_title="RD Data Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 RD Data Dashboard")
st.caption("Interface Streamlit para executar o pipeline e visualizar os dados processados.")

st.sidebar.header("Parâmetros de execução")

default_cfg = get_config()

uploaded_rmd = st.sidebar.file_uploader(
    "Envie o arquivo RMD (.xlsx)",
    type=["xlsx"],
)

output_name = st.sidebar.text_input(
    "Nome do arquivo Excel de saída",
    value=default_cfg.get("OUTPUT_NAME", "Recent Developments Data.xlsx"),
)

start_sgs = st.sidebar.text_input(
    "Data inicial do SGS",
    value=default_cfg.get("START_SGS", "2019-01-01"),
)

days_daily_sgs = st.sidebar.number_input(
    "Janela diária SGS (dias)",
    min_value=1,
    max_value=60,
    value=int(default_cfg.get("DAYS_DAILY_SGS", 7)),
    step=1,
)

mes_alvo = st.sidebar.selectbox(
    "Mês-alvo do RMD (usado apenas no modo 'mês-alvo')",
    options=["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"],
    index=["Jan", "Fev", "Mar", "Abr", "Mai", "Jun", "Jul", "Ago", "Set", "Out", "Nov", "Dez"].index(
        default_cfg.get("MES_ALVO", "Dez")
    ),
)

ano_inicio_rmd = st.sidebar.number_input(
    "Ano inicial do RMD",
    min_value=2000,
    max_value=2100,
    value=int(default_cfg.get("ANO_INICIO_RMD", 2020)),
    step=1,
)

aba_rmd = st.sidebar.text_input(
    "Aba do RMD",
    value=default_cfg.get("ABA_RMD", "2.1"),
)

rmd_extraction_mode_label = st.sidebar.selectbox(
    "Modo de extração anual do DPF",
    options=[
        "Último mês disponível por ano",
        "Mês-alvo por ano",
    ],
    index=0,
)

rmd_extraction_mode = (
    "ultimo_disponivel"
    if rmd_extraction_mode_label == "Último mês disponível por ano"
    else "mes_alvo"
)

run_button = st.sidebar.button("Executar pipeline", type="primary")

if run_button:
    if uploaded_rmd is None:
        st.error("Envie primeiro o arquivo RMD (.xlsx).")
    else:
        with st.spinner("Executando coleta, processamento e exportação..."):
            result = run_pipeline(
                rmd_uploaded_file=uploaded_rmd,
                output_name=output_name,
                start_sgs=start_sgs,
                days_daily_sgs=days_daily_sgs,
                mes_alvo=mes_alvo,
                ano_inicio_rmd=ano_inicio_rmd,
                aba_rmd=aba_rmd,
                rmd_extraction_mode=rmd_extraction_mode,
            )

        st.session_state["rd_result"] = result

result = st.session_state.get("rd_result")

if result:
    if result["success"]:
        st.success("Pipeline executado com sucesso.")

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
        export_tables = result["export_tables"]

        tab_names = list(export_tables.keys())
        tabs = st.tabs(tab_names)

        for tab, sheet_name in zip(tabs, tab_names):
            with tab:
                df = export_tables[sheet_name]

                st.write(f"**Aba:** {sheet_name}")
                st.write(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")

                col_a, col_b = st.columns([2, 1])

                with col_a:
                    modo_visualizacao = st.selectbox(
                        "Visualização",
                        options=["recentes", "antigas", "completo"],
                        index=0,
                        key=f"modo_{sheet_name}",
                        format_func=lambda x: {
                            "recentes": "Mais recentes primeiro",
                            "antigas": "Mais antigas primeiro",
                            "completo": "Tabela completa",
                        }[x],
                    )

                with col_b:
                    max_rows = st.number_input(
                        "Linhas",
                        min_value=5,
                        max_value=500,
                        value=50,
                        step=5,
                        key=f"max_rows_{sheet_name}",
                    )

                df_view = prepare_preview_df(df, modo_visualizacao, max_rows)
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
        st.error("A execução falhou.")
        st.code(result["error"])

        with st.expander("Logs da execução", expanded=True):
            for msg in result["logs"]:
                st.text(msg)

else:
    st.info("Envie o arquivo RMD, ajuste os parâmetros na barra lateral e clique em **Executar pipeline**.")