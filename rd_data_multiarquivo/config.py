from __future__ import annotations


def get_config() -> dict:
    pt_meses = {
        "Jan": 1, "Fev": 2, "Mar": 3, "Abr": 4, "Mai": 5, "Jun": 6,
        "Jul": 7, "Ago": 8, "Set": 9, "Out": 10, "Nov": 11, "Dez": 12,
    }

    series_mensais = {
        # preços
        "IPCA_acumulado_doze_meses": 13522,
        "IGP_DI_mensal": 190,
        "Selic_acumulada_no_mes": 4390,
        "Reservas_estoque": 3546,
        "Cambio_medio_mensal": 3698,
        "TJLP": 256,
        "TLP": 27572,
        "Inadimplencia_total": 21082,
        "Inadimplencia_pessoas_fisicas": 21112,
        "Inadimplencia_pessoas_juridicas": 21086,
        "Inadimplencia_recursos_livres": 21085,

        # setor externo mensal
        "Transacoes_correntes": 22701,
        "Transacoes_correntes_%_do_PIB": 23079,
        "Conta_capital": 22851,
        "Conta_financeira": 22863,
        "IDP": 22885,
        "IDP_%_do_PIB": 23080,

        # fiscal
        "ResultadoPrimGovernoCentral": 5497,
        "ResultadoPrimConsolidado": 5793,
        "ResultadoGovCentralNominalCorrente": 4573,
        "ResultadoGovCentralPrimarioCorrente": 4639,
        "ResultadoGovCentralNominal12m": 5002,
        "ResultadoGovCentralPrimario12m": 5068,
        "ResultadoSetorPublicoConsolidadoNominalCorrente": 4583,
        "ResultadoSetorPublicoConsolidadoPrimarioCorrente": 4649,
        "ResultadoSetorPublicoConsolidadoNominal12m": 5012,
        "ResultadoSetorPublicoConsolidadoPrimario12m": 5078,
        "DBGG": 13761,
        "DBGG_PIB": 13762,
        "DLSP": 4478,
        "DLSP_PIB": 4513,
    }

    series_diarias = {
        "Cambio_diario": 1
    }

    series_anuais = {
        "PIB_RS": 1207,
        "PIB_US": 7324,
        "PIBcresc": 7326,
        "pibpercaptaUS": 21776,
        "Cambio_fim": 3692,
        "Cambio_medio": 3694,
        "Export": 23468,
        "Import": 23469,
        "TransacoesCorrentes": 23461,
        "ContaCapital": 23611,
        "ContaFinanceira": 23623,
        "AtivoReservas": 23803,
        "ReservasInternacionais": 3545,
    }

    return {
        # Coleta
        "START_SGS": "2019-01-01",
        "DAYS_DAILY_SGS": 7,

        # Agora aponta para a pasta-base; o app localiza automaticamente o RMD mais recente
        "ARQUIVO_RMD": "rmd/",
        "ABA_RMD": "2.1",
        "MES_ALVO": "Dez",
        "ANO_INICIO_RMD": 2020,

        # Saída
        "OUTPUT_NAME": "Recent Developments Data.xlsx",

        # Auxiliares
        "PT_MESES": pt_meses,
        "series_mensais": series_mensais,
        "series_diarias": series_diarias,
        "series_anuais": series_anuais,

        "EXTERNO_MENSAL_KEYS": [
            "Transacoes_correntes",
            "Transacoes_correntes_%_do_PIB",
            "Conta_capital",
            "Conta_financeira",
            "IDP",
            "IDP_%_do_PIB",
        ],
        "EXTERNO_12M_KEYS": [
            "Transacoes_correntes",
            "Conta_capital",
            "Conta_financeira",
            "IDP",
        ],

        # Logging
        "LOG_LEVEL": "INFO",
        "LOG_TO_CONSOLE": True,
        "LOG_TO_FILE": True,
        "LOG_DIR": "logs",
        "LOG_FILE_BASENAME": "rd_data",
        "LOG_ERROR_FILE_BASENAME": "rd_data_errors",
        "LOG_FILE_TIMESTAMP_FORMAT": "%Y%m%d_%H%M%S",
        "ENABLE_ROTATING_CURRENT_LOG": True,
        "ROTATING_LOG_NAME": "rd_data_current.log",
        "ROTATING_MAX_BYTES": 1_000_000,
        "ROTATING_BACKUP_COUNT": 5,
    }
