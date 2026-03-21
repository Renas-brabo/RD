from __future__ import annotations

import pandas as pd


def require_columns(df: pd.DataFrame, required_cols: list[str], df_name: str):
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name}: colunas obrigatórias ausentes: {missing}")


def warn_all_nan(df: pd.DataFrame, cols: list[str], df_name: str, warnings: list[str]):
    for col in cols:
        if col in df.columns and df[col].notna().sum() == 0:
            warnings.append(f"{df_name}: coluna '{col}' está totalmente vazia.")


def validate_config(cfg: dict):
    required_keys = [
        "START_SGS",
        "ARQUIVO_RMD",
        "ABA_RMD",
        "MES_ALVO",
        "ANO_INICIO_RMD",
        "OUTPUT_NAME",
        "PT_MESES",
        "series_mensais",
        "series_diarias",
        "series_anuais",
        "EXTERNO_MENSAL_KEYS",
        "EXTERNO_12M_KEYS",
    ]
    missing = [k for k in required_keys if k not in cfg]
    if missing:
        raise ValueError(f"Configuração incompleta. Chaves ausentes: {missing}")

    for key in ["series_mensais", "series_diarias", "series_anuais"]:
        if not isinstance(cfg[key], dict) or not cfg[key]:
            raise ValueError(f"'{key}' precisa ser um dicionário não vazio.")

    all_series_ids = (
        list(cfg["series_mensais"].values())
        + list(cfg["series_diarias"].values())
        + list(cfg["series_anuais"].values())
    )
    non_int = [x for x in all_series_ids if not isinstance(x, int)]
    if non_int:
        raise ValueError(f"Há identificadores SGS não inteiros na configuração: {non_int}")


def validate_raw_data(raw: dict):
    required_raw_keys = [
        "dados_mensais_raw",
        "dados_diarios_raw",
        "dados_anuais_raw",
        "pop",
        "desemp",
        "desemp_trimestre_movel",
        "out_dpf_raw",
    ]
    missing = [k for k in required_raw_keys if k not in raw]
    if missing:
        raise ValueError(f"Dados brutos incompletos. Chaves ausentes: {missing}")

    if raw["dados_mensais_raw"].empty:
        raise ValueError("A coleta mensal retornou DataFrame vazio.")
    if raw["dados_diarios_raw"].empty:
        raise ValueError("A coleta diária retornou DataFrame vazio.")
    if raw["dados_anuais_raw"].empty:
        raise ValueError("A coleta anual retornou DataFrame vazio.")
    if raw["pop"].empty or raw["desemp"].empty:
        raise ValueError("A coleta SIDRA retornou DataFrame vazio para população ou desemprego.")
    if raw["desemp_trimestre_movel"].empty:
        raise ValueError("A coleta SIDRA retornou DataFrame vazio para desemprego em trimestre móvel.")
    if raw["out_dpf_raw"].empty:
        raise ValueError("A extração do RMD retornou DataFrame vazio para DPF.")

    require_columns(raw["dados_mensais_raw"], ["Reservas_estoque", "Cambio_medio_mensal"], "dados_mensais_raw")
    require_columns(raw["dados_diarios_raw"], ["Cambio_diario"], "dados_diarios_raw")
    require_columns(raw["dados_anuais_raw"], ["PIB_RS", "PIB_US", "Cambio_fim", "Cambio_medio"], "dados_anuais_raw")
    require_columns(raw["pop"], ["Data", "Populacao"], "pop")
    require_columns(raw["desemp"], ["Data", "Taxa_Desemprego"], "desemp")
    require_columns(
        raw["desemp_trimestre_movel"],
        ["Data", "Taxa_Desocupacao_Trimestre_Movel"],
        "desemp_trimestre_movel",
    )
    require_columns(raw["out_dpf_raw"], ["Ano", "DPF", "DPMFi", "DPFe"], "out_dpf_raw")


def validate_processed_data(processed: dict) -> list[str]:
    warnings: list[str] = []

    required_processed_keys = [
        "dados_precos",
        "dados_externo_mensal",
        "dados_fiscal",
        "cambio_diario",
        "dados_precos_anuais",
        "pib_anual",
        "bp_anual",
        "out_dpf",
        "desemp_anual",
        "pop",
        "desemp",
    ]
    missing = [k for k in required_processed_keys if k not in processed]
    if missing:
        raise ValueError(f"Dados processados incompletos. Chaves ausentes: {missing}")

    require_columns(
        processed["dados_precos"],
        [
            "IPCA_acumulado_doze_meses",
            "IGP_DI_mensal",
            "Selic_acumulada_no_mes",
            "Reservas_estoque",
            "Variacao_reservas_estoque_mensal_%",
            "Cambio_medio_mensal",
            "Variacao_cambial_mensal_%",
            "Taxa_Desocupacao_Trimestre_Movel",
        ],
        "dados_precos",
    )

    require_columns(
        processed["dados_externo_mensal"],
        [
            "Transacoes_correntes",
            "Transacoes_correntes_%_do_PIB",
            "Conta_capital",
            "Conta_financeira",
            "IDP",
            "IDP_%_do_PIB",
        ],
        "dados_externo_mensal",
    )

    require_columns(
        processed["dados_fiscal"],
        [
            "ResultadoPrimGovernoCentral",
            "ResultadoPrimConsolidado",
            "ResultadoGovCentralNominalCorrente",
            "ResultadoGovCentralPrimarioCorrente",
            "ResultadoGovCentralNominal12m",
            "ResultadoGovCentralPrimario12m",
        ],
        "dados_fiscal",
    )

    require_columns(
        processed["cambio_diario"],
        ["Data", "Cambio_diario", "Variacao_cambial_diaria_%"],
        "cambio_diario",
    )

    require_columns(
        processed["dados_precos_anuais"],
        ["IGP_DI_anual_%", "Selic_anual_%", "IPCA_anual_%", "Juro_real_anual_%"],
        "dados_precos_anuais",
    )

    require_columns(
        processed["pib_anual"],
        ["PIB_RS", "PIB_US", "Cambio_fim", "Cambio_medio"],
        "pib_anual",
    )

    require_columns(
        processed["bp_anual"],
        ["Export", "Import", "TransacoesCorrentes", "ContaCapital", "ContaFinanceira"],
        "bp_anual",
    )

    require_columns(
        processed["out_dpf"],
        ["Ano", "DPF", "DPMFi", "DPFe", "DPF_em_%_do_PIB"],
        "out_dpf",
    )

    require_columns(
        processed["desemp_anual"],
        ["Ano", "Desemprego_Media_Anual"],
        "desemp_anual",
    )

    warn_all_nan(
        processed["dados_precos"],
        [
            "Variacao_reservas_estoque_mensal_%",
            "Variacao_cambial_mensal_%",
            "Taxa_Desocupacao_Trimestre_Movel",
        ],
        "dados_precos",
        warnings,
    )

    warn_all_nan(
        processed["dados_externo_mensal"],
        ["Transacoes_correntes_%_do_PIB", "IDP_%_do_PIB"],
        "dados_externo_mensal",
        warnings,
    )

    warn_all_nan(
        processed["dados_fiscal"],
        ["ResultadoGovCentralNominal12m", "ResultadoSetorPublicoConsolidadoNominal12m"],
        "dados_fiscal",
        warnings,
    )

    return warnings


def validate_export_tables(export_tables: dict):
    required_sheets = [
        "Mensal_Monetario",
        "Mensal_Fiscal",
        "câmbio diário",
        "Mensal_Externo",
        "Trimestral_Pop_Desemp",
        "PIB_e_outros",
        "Anual_Precos_Juros",
        "Anual_SetorExterno",
    ]
    missing = [s for s in required_sheets if s not in export_tables]
    if missing:
        raise ValueError(f"Abas de exportação ausentes: {missing}")

    for name, df in export_tables.items():
        if not isinstance(df, pd.DataFrame):
            raise ValueError(f"A aba '{name}' não é um DataFrame.")
        if df.empty:
            raise ValueError(f"A aba '{name}' está vazia.")