from __future__ import annotations

import logging
import time
from pathlib import Path

from .logging_utils import LogArtifacts, base_dir
from .utils import to_col_data, ajustar_largura_colunas
from .validators import validate_export_tables


def build_export_tables(processed: dict, logger: logging.Logger) -> dict:
    logger.info("Montando tabelas finais de exportação.")

    mensal_monetario = (
        to_col_data(processed["dados_precos"], "Data")
        .sort_values("Data")
        .reset_index(drop=True)
    )

    colunas_reservas = [
        "Data",
        "Reservas_estoque",
        "Variacao_reservas_estoque_mensal_%",
    ]
    reservas_mensais = mensal_monetario[colunas_reservas].copy()

    mensal_monetario = mensal_monetario.drop(
        columns=["Reservas_estoque", "Variacao_reservas_estoque_mensal_%"],
        errors="ignore",
    )

    mensal_fiscal = (
        to_col_data(processed["dados_fiscal"], "Data")
        .sort_values("Data")
        .reset_index(drop=True)
    )

    mensal_externo = (
        to_col_data(processed["dados_externo_mensal"], "Data")
        .sort_values("Data")
        .reset_index(drop=True)
    )
    mensal_externo = mensal_externo.merge(reservas_mensais, on="Data", how="left")

    trimestral_populacao_desemprego = (
        processed["pop"]
        .merge(processed["desemp"], on="Data", how="outer")
        .sort_values("Data")
        .reset_index(drop=True)
    )

    anual_pib_e_outros = (
        processed["pib_anual"]
        .reset_index()
        .sort_values("Ano")
        .reset_index(drop=True)
        .merge(
            processed["out_dpf"].copy().sort_values("Ano").reset_index(drop=True),
            on="Ano",
            how="left",
        )
        .merge(
            processed["desemp_anual"].copy().sort_values("Ano").reset_index(drop=True),
            on="Ano",
            how="left",
        )
    )

    anual_precos_juros = (
        processed["dados_precos_anuais"]
        .reset_index()
        .sort_values("Ano")
        .reset_index(drop=True)
    )

    anual_setor_externo = (
        processed["bp_anual"]
        .reset_index()
        .sort_values("Ano")
        .reset_index(drop=True)
    )

    export_tables = {
        "Mensal_Monetario": mensal_monetario,
        "Mensal_Fiscal": mensal_fiscal,
        "câmbio diário": processed["cambio_diario"],
        "Mensal_Externo": mensal_externo,
        "Trimestral_Pop_Desemp": trimestral_populacao_desemprego,
        "PIB_e_outros": anual_pib_e_outros,
        "Anual_Precos_Juros": anual_precos_juros,
        "Anual_SetorExterno": anual_setor_externo,
    }

    validate_export_tables(export_tables)
    logger.info("Tabelas de exportação montadas e validadas com sucesso.")
    return export_tables


def export_to_excel(export_tables: dict, output_name: str, logger: logging.Logger) -> Path:
    output = base_dir() / output_name
    logger.info("Iniciando exportação para Excel: %s", output)

    import pandas as pd

    with pd.ExcelWriter(
        output,
        engine="openpyxl",
        datetime_format="YYYY-MM-DD",
        date_format="YYYY-MM-DD",
        mode="w",
    ) as writer:
        for nome_aba, df_aba in export_tables.items():
            nome_aba_excel = nome_aba[:31]
            logger.info(
                "Exportando aba '%s' com %s linhas e %s colunas.",
                nome_aba_excel,
                len(df_aba),
                len(df_aba.columns),
            )
            df_aba.to_excel(writer, sheet_name=nome_aba_excel, index=False)
            ws = writer.sheets[nome_aba_excel]
            ws.freeze_panes = "B2"
            ajustar_largura_colunas(ws, df_aba)

    logger.info("Exportação concluída com sucesso.")
    return output


def build_execution_summary(
    export_tables: dict,
    warnings: list[str],
    output: Path,
    logs: LogArtifacts,
    started_at: float,
) -> dict:
    elapsed_seconds = round(time.time() - started_at, 3)
    total_rows_exported = int(sum(len(df) for df in export_tables.values()))
    total_columns_exported = int(sum(len(df.columns) for df in export_tables.values()))

    return {
        "output_excel": str(output),
        "execution_log": str(logs.execution_log) if logs.execution_log else None,
        "error_log": str(logs.error_log) if logs.error_log else None,
        "rotating_log": str(logs.rotating_log) if logs.rotating_log else None,
        "sheets_exported": list(export_tables.keys()),
        "sheet_count": len(export_tables),
        "total_rows_exported": total_rows_exported,
        "total_columns_exported": total_columns_exported,
        "warning_count": len(warnings),
        "elapsed_seconds": elapsed_seconds,
    }


def log_execution_summary(logger: logging.Logger, summary: dict, warnings: list[str]):
    logger.info("Resumo final da execução:")
    logger.info("- arquivo Excel: %s", summary["output_excel"])
    logger.info("- arquivo de log da execução: %s", summary["execution_log"])
    logger.info("- arquivo de log de erros: %s", summary["error_log"])
    if summary["rotating_log"]:
        logger.info("- arquivo de log rotativo: %s", summary["rotating_log"])
    logger.info("- total de abas exportadas: %s", summary["sheet_count"])
    logger.info("- total de linhas exportadas: %s", summary["total_rows_exported"])
    logger.info("- total de colunas exportadas: %s", summary["total_columns_exported"])
    logger.info("- tempo total (s): %s", summary["elapsed_seconds"])

    if warnings:
        logger.warning("Validações concluídas com avisos (%s aviso(s)).", len(warnings))
        for w in warnings:
            logger.warning(w)
    else:
        logger.info("Validações concluídas sem avisos.")