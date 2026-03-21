from __future__ import annotations

import logging

from .validators import validate_export_tables


def get_rename_maps() -> dict:
    rename_common = {"Data": "data", "Ano": "ano"}

    return {
        "Mensal_Monetario": {
            **rename_common,
            "IPCA_acumulado_doze_meses": "ipca_acumulado_12_meses",
            "IGP_DI_mensal": "igp_di_mensal",
            "Selic_acumulada_no_mes": "selic_acumulada_mes",
            "Cambio_medio_mensal": "cambio_medio_mensal",
            "Variacao_cambial_mensal_%": "variacao_cambial_mensal_percentual",
            "TJLP": "tjlp",
            "TLP": "tlp",
            "Inadimplencia_total": "inadimplencia_total_percentual",
            "Inadimplencia_pessoas_fisicas": "inadimplencia_pessoas_fisicas_percentual",
            "Inadimplencia_pessoas_juridicas": "inadimplencia_pessoas_juridicas_percentual",
            "Inadimplencia_recursos_livres": "inadimplencia_recursos_livres_percentual",
            "Taxa_Desocupacao_Trimestre_Movel": "taxa_desemprego",
        },
        "Mensal_Fiscal": {
            **rename_common,
            "ResultadoPrimGovernoCentral": "resultado_governo_central_primario_percentual_pib",
            "ResultadoPrimConsolidado": "resultado_setor_publico_consolidado_primario_percentual_pib",
            "ResultadoGovCentralNominalCorrente": "resultado_governo_central_nominal_corrente",
            "ResultadoGovCentralPrimarioCorrente": "resultado_governo_central_primario_corrente",
            "ResultadoGovCentralNominal12m": "resultado_governo_central_nominal_12_meses",
            "ResultadoGovCentralPrimario12m": "resultado_governo_central_primario_12_meses",
            "ResultadoSetorPublicoConsolidadoNominalCorrente": "resultado_setor_publico_consolidado_nominal_corrente",
            "ResultadoSetorPublicoConsolidadoPrimarioCorrente": "resultado_setor_publico_consolidado_primario_corrente",
            "ResultadoSetorPublicoConsolidadoNominal12m": "resultado_setor_publico_consolidado_nominal_12_meses",
            "ResultadoSetorPublicoConsolidadoPrimario12m": "resultado_setor_publico_consolidado_primario_12_meses",
            "DBGG": "dbgg",
            "DBGG_PIB": "dbgg_percentual_pib",
            "DLSP": "dlsp",
            "DLSP_PIB": "dlsp_percentual_pib",
        },
        "Mensal_Externo": {
            **rename_common,
            "Transacoes_correntes": "transacoes_correntes_corrente",
            "Transacoes_correntes_%_do_PIB": "transacoes_correntes_percentual_pib",
            "Conta_capital": "conta_capital_corrente",
            "Conta_financeira": "conta_financeira_corrente",
            "IDP": "idp_corrente",
            "IDP_%_do_PIB": "idp_percentual_pib",
            "Transacoes_correntes_acumulada_em_12_meses": "transacoes_correntes_acumulado_12_meses",
            "Conta_capital_acumulada_em_12_meses": "conta_capital_acumulado_12_meses",
            "Conta_financeira_acumulada_em_12_meses": "conta_financeira_acumulado_12_meses",
            "IDP_acumulado_em_12_meses": "idp_acumulado_12_meses",
            "Reservas_estoque": "reservas_internacionais_estoque",
            "Variacao_reservas_estoque_mensal_%": "variacao_reservas_internacionais_mensal_percentual",
        },
        "câmbio diário": {
            **rename_common,
            "Cambio_diario": "cambio_diario",
            "Variacao_cambial_diaria_%": "variacao_cambial_diaria_percentual",
        },
        "PIB_e_outros": {
            **rename_common,
            "PIB_RS": "pib_reais_bilhoes",
            "PIB_US": "pib_dolares_bilhoes",
            "PIBcresc": "crescimento_pib_percentual",
            "pibpercaptaUS": "pib_per_capita_dolares",
            "Cambio_fim": "cambio_fim_periodo",
            "Cambio_medio": "cambio_medio",
            "variacao_cambial": "variacao_cambial",
            "DPF": "dpf",
            "DPMFi": "dpmfi",
            "DPFe": "dpfe",
            "DPF_em_%_do_PIB": "dpf_percentual_pib",
            "Desemprego_Media_Anual": "desemprego_medio_anual",
        },
        "Anual_Precos_Juros": {
            **rename_common,
            "IGP_DI_anual_%": "igp_di_anual_percentual",
            "Selic_anual_%": "selic_anual_percentual",
            "IPCA_anual_%": "ipca_anual_percentual",
            "Juro_real_anual_%": "juro_real_anual_percentual",
        },
        "Anual_SetorExterno": {
            **rename_common,
            "Export": "exportacoes",
            "Import": "importacoes",
            "TransacoesCorrentes": "transacoes_correntes",
            "ContaCapital": "conta_capital",
            "ContaFinanceira": "conta_financeira",
            "AtivoReservas": "ativo_reservas",
            "ReservasInternacionais": "reservas_internacionais",
        },
    }


def standardize_column_names(export_tables: dict, logger: logging.Logger) -> dict:
    logger.info("Padronizando nomenclatura das colunas exportadas.")
    rename_maps = get_rename_maps()
    standardized = {
        sheet_name: df.rename(columns=rename_maps.get(sheet_name, {}))
        for sheet_name, df in export_tables.items()
    }
    validate_export_tables(standardized)
    logger.info("Padronização de nomenclatura concluída.")
    return standardized