from __future__ import annotations

import time

from .config import get_config
from .logging_utils import setup_logger
from .validators import validate_config
from .collectors import collect_data
from .processors import process_data
from .exporters import build_export_tables, export_to_excel, build_execution_summary, log_execution_summary
from .naming import standardize_column_names


def main():
    started_at = time.time()
    cfg = get_config()
    logger, log_artifacts = setup_logger(cfg)
    logger.info("Iniciando execução do script Recent Developments Data.")
    if log_artifacts.execution_log is not None:
        logger.info("Arquivo de log desta execução: %s", log_artifacts.execution_log)
    if log_artifacts.error_log is not None:
        logger.info("Arquivo de log de erros desta execução: %s", log_artifacts.error_log)
    if log_artifacts.rotating_log is not None:
        logger.info("Arquivo de log rotativo corrente: %s", log_artifacts.rotating_log)

    try:
        validate_config(cfg)
        logger.info("Configuração validada com sucesso.")

        raw = collect_data(cfg, logger)
        processed, warnings = process_data(raw, cfg, logger)
        export_tables = build_export_tables(processed, logger)
        export_tables = standardize_column_names(export_tables, logger)
        output = export_to_excel(export_tables, cfg["OUTPUT_NAME"], logger)

        summary = build_execution_summary(export_tables, warnings, output, log_artifacts, started_at)
        log_execution_summary(logger, summary, warnings)

    except Exception as exc:
        logger.exception("Falha na execução do script: %s", exc)
        raise


if __name__ == "__main__":
    main()
