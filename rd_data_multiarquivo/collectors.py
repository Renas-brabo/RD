from __future__ import annotations

import logging
from datetime import date, timedelta

from .utils import (
    fetch_sgs,
    sidra_trimestral,
    sidra_trimestre_movel_mensal,
    extrai_dpf_dez,
)
from .validators import validate_raw_data


def collect_data(cfg: dict, logger: logging.Logger) -> dict:
    logger.info("Iniciando coleta de dados SGS mensais, diários e anuais.")

    dados_mensais_raw = fetch_sgs(cfg["series_mensais"], cfg["START_SGS"])
    logger.info(
        "Coleta mensal concluída com %s linhas e %s colunas.",
        len(dados_mensais_raw),
        len(dados_mensais_raw.columns),
    )

    dias_cambio = int(cfg.get("DAYS_DAILY_SGS", 7))
    daily_start = (date.today() - timedelta(days=dias_cambio)).isoformat()

    dados_diarios_raw = fetch_sgs(cfg["series_diarias"], daily_start)
    logger.info(
        "Coleta diária concluída com %s linhas e %s colunas (janela iniciada em %s).",
        len(dados_diarios_raw),
        len(dados_diarios_raw.columns),
        daily_start,
    )

    dados_anuais_raw = fetch_sgs(cfg["series_anuais"], cfg["START_SGS"])
    logger.info(
        "Coleta anual concluída com %s linhas e %s colunas.",
        len(dados_anuais_raw),
        len(dados_anuais_raw.columns),
    )

    logger.info("Coletando séries SIDRA de população e desemprego.")
    pop = sidra_trimestral("6462", "606", "last 24", "Populacao")
    desemp = sidra_trimestral("4099", "4099", "last 20", "Taxa_Desemprego")

    logger.info("Coletando série SIDRA de taxa de desocupação em trimestre móvel mensal (tabela 6381).")
    desemp_trimestre_movel = sidra_trimestre_movel_mensal(
        "6381",
        "last 24",
        "Taxa_Desocupacao_Trimestre_Movel",
    )

    logger.info("Extraindo DPF do arquivo RMD: %s", cfg["ARQUIVO_RMD"])
    out_dpf_raw = extrai_dpf_dez(
        cfg["ARQUIVO_RMD"],
        cfg["ABA_RMD"],
        cfg["MES_ALVO"],
        cfg["ANO_INICIO_RMD"],
        cfg["PT_MESES"],
    )

    raw = {
        "dados_mensais_raw": dados_mensais_raw,
        "dados_diarios_raw": dados_diarios_raw,
        "dados_anuais_raw": dados_anuais_raw,
        "pop": pop,
        "desemp": desemp,
        "desemp_trimestre_movel": desemp_trimestre_movel,
        "out_dpf_raw": out_dpf_raw,
    }

    validate_raw_data(raw)
    logger.info("Validação dos dados brutos concluída com sucesso.")
    return raw