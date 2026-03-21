from __future__ import annotations

import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path


@dataclass
class LogArtifacts:
    execution_log: Path | None
    error_log: Path | None
    rotating_log: Path | None


def base_dir() -> Path:
    try:
        return Path(__file__).resolve().parent
    except NameError:
        return Path.cwd()


def build_log_paths(cfg: dict) -> LogArtifacts:
    log_dir = base_dir() / str(cfg.get("LOG_DIR", "logs"))
    log_dir.mkdir(parents=True, exist_ok=True)

    ts_fmt = str(cfg.get("LOG_FILE_TIMESTAMP_FORMAT", "%Y%m%d_%H%M%S"))
    timestamp = datetime.now().strftime(ts_fmt)
    exec_base = str(cfg.get("LOG_FILE_BASENAME", "rd_data"))
    error_base = str(cfg.get("LOG_ERROR_FILE_BASENAME", "rd_data_errors"))

    execution_log = log_dir / f"{exec_base}_{timestamp}.log"
    error_log = log_dir / f"{error_base}_{timestamp}.log"

    rotating_log = None
    if cfg.get("ENABLE_ROTATING_CURRENT_LOG", False):
        current_dir = log_dir / "current"
        current_dir.mkdir(parents=True, exist_ok=True)
        rotating_log = current_dir / str(cfg.get("ROTATING_LOG_NAME", "rd_data_current.log"))

    return LogArtifacts(execution_log=execution_log, error_log=error_log, rotating_log=rotating_log)


def setup_logger(cfg: dict, name: str = "rd_data") -> tuple[logging.Logger, LogArtifacts]:
    level_name = str(cfg.get("LOG_LEVEL", "INFO")).upper()
    level = getattr(logging, level_name, logging.INFO)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False

    if logger.handlers:
        logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    artifacts = build_log_paths(cfg)

    if cfg.get("LOG_TO_CONSOLE", True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    if cfg.get("LOG_TO_FILE", True):
        info_handler = logging.FileHandler(artifacts.execution_log, mode="a", encoding="utf-8")
        info_handler.setLevel(level)
        info_handler.setFormatter(formatter)
        logger.addHandler(info_handler)

        error_handler = logging.FileHandler(artifacts.error_log, mode="a", encoding="utf-8")
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(formatter)
        logger.addHandler(error_handler)

    if artifacts.rotating_log is not None:
        rotating_handler = RotatingFileHandler(
            artifacts.rotating_log,
            maxBytes=int(cfg.get("ROTATING_MAX_BYTES", 1_000_000)),
            backupCount=int(cfg.get("ROTATING_BACKUP_COUNT", 5)),
            encoding="utf-8",
        )
        rotating_handler.setLevel(level)
        rotating_handler.setFormatter(formatter)
        logger.addHandler(rotating_handler)

    return logger, artifacts
