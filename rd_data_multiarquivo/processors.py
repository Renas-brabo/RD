from __future__ import annotations

import logging

import numpy as np
import pandas as pd

from .utils import scale_guard, december_or_last, annualize
from .validators import validate_processed_data


def prepare_annual_views(dados_anuais: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    dados_anuais = dados_anuais.copy()

    dados_anuais["PIB_RS"] = scale_guard(dados_anuais["PIB_RS"], moeda="R$", to="bilhoes")
    dados_anuais["PIB_US"] = scale_guard(dados_anuais["PIB_US"], moeda="US$", to="bilhoes")

    for col in [
        "Export",
        "Import",
        "TransacoesCorrentes",
        "ContaCapital",
        "ContaFinanceira",
        "AtivoReservas",
        "ReservasInternacionais",
    ]:
        if col in dados_anuais.columns:
            dados_anuais[col] = scale_guard(dados_anuais[col], moeda="US$", to="bilhoes")

    if "Cambio_fim" in dados_anuais.columns:
        dados_anuais["variacao_cambial"] = pd.to_numeric(
            dados_anuais["Cambio_fim"], errors="coerce"
        ).pct_change()
    else:
        dados_anuais["variacao_cambial"] = np.nan

    dados_pib = dados_anuais[
        ["PIB_RS", "PIB_US", "PIBcresc", "pibpercaptaUS", "Cambio_fim", "Cambio_medio", "variacao_cambial"]
    ].copy()

    dados_bp = dados_anuais[
        ["Export", "Import", "TransacoesCorrentes", "ContaCapital", "ContaFinanceira", "AtivoReservas", "ReservasInternacionais"]
    ].copy()

    return dados_pib, dados_bp


def prepare_daily_exchange(dados_diarios: pd.DataFrame) -> pd.DataFrame:
    if "Cambio_diario" not in dados_diarios.columns:
        return pd.DataFrame(columns=["Data", "Cambio_diario", "Variacao_cambial_diaria_%"])

    dados_diarios = dados_diarios.copy().sort_index()
    dados_diarios["Cambio_diario"] = pd.to_numeric(dados_diarios["Cambio_diario"], errors="coerce")
    dados_diarios["Variacao_cambial_diaria_%"] = dados_diarios["Cambio_diario"].pct_change() * 100

    out = (
        dados_diarios[["Cambio_diario", "Variacao_cambial_diaria_%"]]
        .dropna(how="all")
        .tail(7)
        .reset_index()
    )

    out = out.rename(columns={out.columns[0]: "Data"})
    out = out[["Data", "Cambio_diario", "Variacao_cambial_diaria_%"]]
    return out


def prepare_monthly_data(dados_mensais: pd.DataFrame, cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    dados_mensais = dados_mensais.copy()

    if "Reservas_estoque" in dados_mensais.columns:
        dados_mensais["Reservas_estoque"] = scale_guard(
            dados_mensais["Reservas_estoque"], moeda="US$", to="bilhoes"
        )
        dados_mensais["Variacao_reservas_estoque_mensal_%"] = pd.to_numeric(
            dados_mensais["Reservas_estoque"], errors="coerce"
        ).pct_change() * 100
    else:
        dados_mensais["Variacao_reservas_estoque_mensal_%"] = np.nan

    for col in ["DBGG", "DLSP"]:
        if col in dados_mensais.columns:
            dados_mensais[col] = scale_guard(dados_mensais[col], moeda="US$", to="bilhoes")

    if "Cambio_medio_mensal" in dados_mensais.columns:
        dados_mensais["Cambio_medio_mensal"] = pd.to_numeric(
            dados_mensais["Cambio_medio_mensal"], errors="coerce"
        )
        dados_mensais["Variacao_cambial_mensal_%"] = dados_mensais["Cambio_medio_mensal"].pct_change() * 100
    else:
        dados_mensais["Cambio_medio_mensal"] = np.nan
        dados_mensais["Variacao_cambial_mensal_%"] = np.nan

    additional_numeric = [
        "TJLP",
        "TLP",
        "Inadimplencia_total",
        "Inadimplencia_pessoas_fisicas",
        "Inadimplencia_pessoas_juridicas",
        "Inadimplencia_recursos_livres",
        "ResultadoGovCentralNominalCorrente",
        "ResultadoGovCentralPrimarioCorrente",
        "ResultadoGovCentralNominal12m",
        "ResultadoGovCentralPrimario12m",
        "ResultadoSetorPublicoConsolidadoNominalCorrente",
        "ResultadoSetorPublicoConsolidadoPrimarioCorrente",
        "ResultadoSetorPublicoConsolidadoNominal12m",
        "ResultadoSetorPublicoConsolidadoPrimario12m",
    ] + cfg["EXTERNO_MENSAL_KEYS"]

    for col in additional_numeric:
        if col in dados_mensais.columns:
            dados_mensais[col] = pd.to_numeric(dados_mensais[col], errors="coerce")
        else:
            dados_mensais[col] = np.nan

    externo_12m_names = {
        "Transacoes_correntes": "Transacoes_correntes_acumulada_em_12_meses",
        "Conta_capital": "Conta_capital_acumulada_em_12_meses",
        "Conta_financeira": "Conta_financeira_acumulada_em_12_meses",
        "IDP": "IDP_acumulado_em_12_meses",
    }

    for col, new_col in externo_12m_names.items():
        dados_mensais[new_col] = dados_mensais[col].rolling(12, min_periods=12).sum()

    dados_precos = dados_mensais[
        [
            "IPCA_acumulado_doze_meses",
            "IGP_DI_mensal",
            "Selic_acumulada_no_mes",
            "Reservas_estoque",
            "Variacao_reservas_estoque_mensal_%",
            "Cambio_medio_mensal",
            "Variacao_cambial_mensal_%",
            "TJLP",
            "TLP",
            "Inadimplencia_total",
            "Inadimplencia_pessoas_fisicas",
            "Inadimplencia_pessoas_juridicas",
            "Inadimplencia_recursos_livres",
        ]
    ].copy()

    dados_externo_mensal = dados_mensais[
        [
            "Transacoes_correntes",
            "Transacoes_correntes_%_do_PIB",
            "Conta_capital",
            "Conta_financeira",
            "IDP",
            "IDP_%_do_PIB",
            "Transacoes_correntes_acumulada_em_12_meses",
            "Conta_capital_acumulada_em_12_meses",
            "Conta_financeira_acumulada_em_12_meses",
            "IDP_acumulado_em_12_meses",
        ]
    ].copy()

    dados_fiscal = dados_mensais[
        [
            "ResultadoPrimGovernoCentral",
            "ResultadoPrimConsolidado",
            "ResultadoGovCentralNominalCorrente",
            "ResultadoGovCentralPrimarioCorrente",
            "ResultadoGovCentralNominal12m",
            "ResultadoGovCentralPrimario12m",
            "ResultadoSetorPublicoConsolidadoNominalCorrente",
            "ResultadoSetorPublicoConsolidadoPrimarioCorrente",
            "ResultadoSetorPublicoConsolidadoNominal12m",
            "ResultadoSetorPublicoConsolidadoPrimario12m",
            "DBGG",
            "DBGG_PIB",
            "DLSP",
            "DLSP_PIB",
        ]
    ].copy()

    return dados_precos, dados_externo_mensal, dados_fiscal


def prepare_annual_prices(dados_precos: pd.DataFrame) -> pd.DataFrame:
    df = dados_precos.copy()
    df.index = pd.to_datetime(df.index)

    for col in ["Selic_acumulada_no_mes", "IPCA_acumulado_doze_meses", "IGP_DI_mensal"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    fator_selic_anual = (1 + df["Selic_acumulada_no_mes"] / 100).groupby(df.index.year).prod()
    meses_selic = df["Selic_acumulada_no_mes"].groupby(df.index.year).count()
    fator_selic_anual = fator_selic_anual.where(meses_selic == 12)

    def pega_dezembro_ou_nan(s):
        dez = s[s.index.month == 12]
        return dez.iloc[-1] if not dez.empty else np.nan

    ipca_anual_pct = df["IPCA_acumulado_doze_meses"].groupby(df.index.year).apply(pega_dezembro_ou_nan)
    fator_ipca_anual = 1 + ipca_anual_pct / 100

    anos = fator_selic_anual.index.union(fator_ipca_anual.index)
    fator_selic_anual = fator_selic_anual.reindex(anos)
    fator_ipca_anual = fator_ipca_anual.reindex(anos)

    juro_real_anual = (fator_selic_anual / fator_ipca_anual - 1) * 100
    juro_real_anual = juro_real_anual.round(3)
    selic_anual_pct = ((fator_selic_anual - 1) * 100).round(3)

    resultado_anual = pd.DataFrame(
        {
            "Selic_anual_%": selic_anual_pct,
            "IPCA_anual_%": ipca_anual_pct.round(3),
            "Juro_real_anual_%": juro_real_anual,
        }
    )

    igp_di_anual = ((1 + df["IGP_DI_mensal"] / 100).groupby(df.index.year).prod() - 1) * 100
    igp_di_anual = igp_di_anual.round(1).rename("IGP_DI_anual_%")

    out = pd.concat([igp_di_anual, resultado_anual], axis=1).sort_index().reindex(
        columns=["IGP_DI_anual_%", "Selic_anual_%", "IPCA_anual_%", "Juro_real_anual_%"]
    )
    out.index.name = "Ano"
    return out


def prepare_annual_outputs(
    dados_pib: pd.DataFrame,
    dados_bp: pd.DataFrame,
    dados_precos_anuais: pd.DataFrame,
    out_dpf_raw: pd.DataFrame,
    desemp: pd.DataFrame,
) -> dict:
    pib_agg = {
        "PIB_RS": lambda s: s.iloc[-1],
        "PIB_US": lambda s: s.iloc[-1],
        "PIBcresc": lambda s: s.iloc[-1],
        "pibpercaptaUS": lambda s: s.iloc[-1],
        "Cambio_fim": december_or_last,
        "Cambio_medio": pd.Series.mean,
        "variacao_cambial": pd.Series.mean,
    }
    pib_anual = annualize(dados_pib, pib_agg)

    bp_agg = {
        "Export": lambda s: s.iloc[-1] if len(s) else np.nan,
        "Import": lambda s: s.iloc[-1] if len(s) else np.nan,
        "TransacoesCorrentes": lambda s: s.iloc[-1] if len(s) else np.nan,
        "ContaCapital": lambda s: s.iloc[-1] if len(s) else np.nan,
        "ContaFinanceira": lambda s: s.iloc[-1] if len(s) else np.nan,
        "AtivoReservas": december_or_last,
        "ReservasInternacionais": lambda s: s.iloc[-1] if len(s) else np.nan,
    }
    bp_anual = annualize(dados_bp, bp_agg)

    out_dpf = out_dpf_raw.copy()
    out_dpf = out_dpf.merge(pib_anual[["PIB_RS"]].reset_index(), on="Ano", how="left")
    out_dpf["DPF_em_%_do_PIB"] = out_dpf["DPF"] / out_dpf["PIB_RS"]
    out_dpf = out_dpf.drop(columns=["PIB_RS"])

    desemp_anual = (
        desemp.assign(Ano=desemp["Data"].dt.year)
        .groupby("Ano", as_index=False)["Taxa_Desemprego"]
        .mean()
        .rename(columns={"Taxa_Desemprego": "Desemprego_Media_Anual"})
    )
    desemp_anual["Desemprego_Media_Anual"] = desemp_anual["Desemprego_Media_Anual"].round(3)

    macro_anual = pib_anual.join(dados_precos_anuais, how="outer").join(bp_anual, how="outer").sort_index()

    return {
        "pib_anual": pib_anual,
        "bp_anual": bp_anual,
        "out_dpf": out_dpf,
        "desemp_anual": desemp_anual,
        "macro_anual": macro_anual,
    }


def process_data(raw: dict, cfg: dict, logger: logging.Logger) -> tuple[dict, list[str]]:
    logger.info("Iniciando processamento dos dados brutos.")

    dados_anuais = raw["dados_anuais_raw"].copy().sort_index()
    dados_mensais = raw["dados_mensais_raw"].copy().sort_index()
    dados_diarios = raw["dados_diarios_raw"].copy().sort_index()

    dados_pib, dados_bp = prepare_annual_views(dados_anuais)
    logger.info("Views anuais de PIB e setor externo preparadas.")

    cambio_diario = prepare_daily_exchange(dados_diarios)
    logger.info("Bloco de câmbio diário preparado com %s linhas.", len(cambio_diario))

    dados_precos, dados_externo_mensal, dados_fiscal = prepare_monthly_data(dados_mensais, cfg)

    desemp_trimestre_movel = raw["desemp_trimestre_movel"].copy()
    desemp_trimestre_movel["Data"] = pd.to_datetime(desemp_trimestre_movel["Data"], errors="coerce")
    desemp_trimestre_movel = (
        desemp_trimestre_movel
        .dropna(subset=["Data"])
        .set_index("Data")
        .sort_index()
    )
    dados_precos = dados_precos.join(desemp_trimestre_movel, how="left")

    logger.info(
        "Blocos mensais preparados: preços=%s linhas, externo=%s linhas, fiscal=%s linhas.",
        len(dados_precos),
        len(dados_externo_mensal),
        len(dados_fiscal),
    )

    dados_precos_anuais = prepare_annual_prices(dados_precos)
    logger.info("Bloco anual de preços e juros preparado com %s linhas.", len(dados_precos_anuais))

    annual_outputs = prepare_annual_outputs(
        dados_pib,
        dados_bp,
        dados_precos_anuais,
        raw["out_dpf_raw"],
        raw["desemp"],
    )
    logger.info("Saídas anuais complementares preparadas.")

    processed = {
        "dados_precos": dados_precos,
        "dados_externo_mensal": dados_externo_mensal,
        "dados_fiscal": dados_fiscal,
        "cambio_diario": cambio_diario,
        "dados_precos_anuais": dados_precos_anuais,
        **annual_outputs,
        "pop": raw["pop"],
        "desemp": raw["desemp"],
    }

    warnings = validate_processed_data(processed)
    logger.info("Validação dos dados processados concluída.")

    return processed, warnings
