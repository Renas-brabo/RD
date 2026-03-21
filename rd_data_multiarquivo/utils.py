from __future__ import annotations

import re
from io import StringIO
from pathlib import Path

import numpy as np
import pandas as pd
from bcb import sgs
from sidrapy import get_table

PAT_MES_ANO = re.compile(r"^(Jan|Fev|Mar|Abr|Mai|Jun|Jul|Ago|Set|Out|Nov|Dez)/\d{2}$")


def fetch_sgs(series: dict, start: str) -> pd.DataFrame:
    """
    Baixa séries do SGS de forma compatível com versões antigas e novas do python-bcb.
    """
    df = None
    last_exc = None

    try:
        df = sgs.get(series, start=start)
    except Exception as e:
        last_exc = e

    if df is None:
        try:
            df = sgs.get(list(series.values()), start=start)
        except Exception as e:
            last_exc = e

    if df is None:
        raise RuntimeError(f"Falha ao coletar séries SGS. Último erro: {last_exc}")

    if not isinstance(df, pd.DataFrame):
        raise TypeError(f"Resposta inesperada do SGS: {type(df)}")

    df = df.copy()

    rename_map = {}
    for nome, codigo in series.items():
        if codigo in df.columns:
            rename_map[codigo] = nome
        elif str(codigo) in df.columns:
            rename_map[str(codigo)] = nome
    if rename_map:
        df = df.rename(columns=rename_map)

    if len(series) == 1 and len(df.columns) == 1:
        unico_nome = next(iter(series.keys()))
        df.columns = [unico_nome]

    if not isinstance(df.index, pd.DatetimeIndex):
        possible_date_cols = [c for c in df.columns if str(c).lower() in ["data", "date"]]
        if possible_date_cols:
            date_col = possible_date_cols[0]
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True, errors="coerce")
            df = df.set_index(date_col)
        else:
            df.index = pd.to_datetime(df.index, dayfirst=True, errors="coerce")

    df = df[~df.index.isna()].copy()

    for col in df.columns:
        df[col] = df[col].astype(str).str.replace(",", ".", regex=False)
        df[col] = pd.to_numeric(df[col], errors="coerce")

    ordered_cols = [nome for nome in series.keys() if nome in df.columns]
    missing = [nome for nome in series.keys() if nome not in df.columns]

    if not ordered_cols:
        raise ValueError(
            f"Nenhuma das séries esperadas foi retornada. "
            f"Esperadas: {list(series.keys())}; colunas recebidas: {list(df.columns)}"
        )

    if missing:
        print("\n[fetch_sgs] Aviso: séries não encontradas na resposta:")
        for nome in missing:
            print(f" - {nome} (código {series[nome]})")

    return df[ordered_cols].sort_index()


def sidra_trimestral(table_code: str, variable: str, period: str, col_name: str) -> pd.DataFrame:
    raw = get_table(
        table_code=table_code,
        territorial_level="1",
        ibge_territorial_code="all",
        variable=variable,
        period=period,
    )
    raw = pd.DataFrame(raw).iloc[1:].copy()
    df = raw[["D2C", "V"]].copy()
    df.columns = ["Periodo", col_name]
    df["Periodo"] = df["Periodo"].astype(str)
    df["Ano"] = df["Periodo"].str[:4].astype(int)
    df["Tri"] = df["Periodo"].str[-2:].astype(int)
    df["Data"] = pd.to_datetime(
        df["Ano"].astype(str) + "-" + (df["Tri"] * 3 - 2).astype(str) + "-01"
    )
    df[col_name] = df[col_name].astype(str).str.replace(",", ".", regex=False)
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")
    return df[["Data", col_name]].sort_values("Data").reset_index(drop=True)


def sidra_trimestre_movel_mensal(
    table_code: str,
    period: str,
    col_name: str,
) -> pd.DataFrame:
    """
    Coleta uma tabela SIDRA da PNAD Contínua mensal / trimestre móvel
    e converte rótulos como 'nov-dez-jan 2026' para Data='2026-01-01'.

    Ex.: tabela 6381 (taxa de desocupação).
    """
    try:
        raw = get_table(
            table_code=table_code,
            territorial_level="1",
            ibge_territorial_code="all",
            period=period,
        )
    except TypeError:
        raw = get_table(
            table_code=table_code,
            territorial_level="1",
            ibge_territorial_code="all",
            variable="all",
            period=period,
        )

    raw = pd.DataFrame(raw).iloc[1:].copy()

    # Filtra a variável principal quando a tabela vier com várias variáveis
    candidate_var_cols = [c for c in ["D1N", "D2N", "D3N", "D4N"] if c in raw.columns]
    if candidate_var_cols:
        var_col = None
        for c in candidate_var_cols:
            serie = raw[c].astype(str)
            if serie.str.contains("Taxa de desocupação", case=False, na=False).any():
                var_col = c
                break
        if var_col is not None:
            mask_main = raw[var_col].astype(str).str.contains(
                "Taxa de desocupação", case=False, na=False
            )
            mask_exclude = raw[var_col].astype(str).str.contains(
                "Coeficiente de variação|Variação|Situação|Média anual",
                case=False,
                na=False,
            )
            raw = raw[mask_main & ~mask_exclude].copy()

    candidate_period_cols = [c for c in ["D2C", "D3C", "D4C", "D2N", "D3N", "D4N"] if c in raw.columns]
    period_col = None
    for c in candidate_period_cols:
        serie = raw[c].astype(str)
        if serie.str.contains("-", regex=False, na=False).any():
            period_col = c
            break
    if period_col is None:
        raise ValueError(
            f"Não foi possível localizar a coluna de período da tabela {table_code}. "
            f"Colunas recebidas: {list(raw.columns)}"
        )

    if "V" not in raw.columns:
        raise ValueError(
            f"Não foi possível localizar a coluna de valores 'V' da tabela {table_code}. "
            f"Colunas recebidas: {list(raw.columns)}"
        )

    df = raw[[period_col, "V"]].copy()
    df.columns = ["Periodo", col_name]

    meses_pt = {
        "jan": 1,
        "fev": 2,
        "mar": 3,
        "abr": 4,
        "mai": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "set": 9,
        "out": 10,
        "nov": 11,
        "dez": 12,
    }

    def parse_periodo_trimestre_movel(label: str) -> pd.Timestamp:
        s = str(label).strip().lower()
        partes = s.split()
        if len(partes) < 2:
            raise ValueError(f"Formato de período inesperado: {label}")

        ano = int(partes[-1])
        meses_label = partes[-2]  # ex.: nov-dez-jan
        meses = meses_label.split("-")
        mes_final = meses[-1].strip()

        if mes_final not in meses_pt:
            raise ValueError(f"Mês final não reconhecido em '{label}'")

        return pd.Timestamp(year=ano, month=meses_pt[mes_final], day=1)

    df["Data"] = df["Periodo"].apply(parse_periodo_trimestre_movel)
    df[col_name] = df[col_name].astype(str).str.replace(",", ".", regex=False)
    df[col_name] = pd.to_numeric(df[col_name], errors="coerce")

    return df[["Data", col_name]].sort_values("Data").reset_index(drop=True)


def scale_guard(s: pd.Series, *, to="bilhoes", moeda="R$") -> pd.Series:
    x = pd.to_numeric(s, errors="coerce")
    med = x.dropna().median()
    if pd.isna(med):
        return x
    if moeda == "R$" and to == "bilhoes":
        return x / 1e9 if med >= 1e7 else x
    if moeda == "US$" and to == "bilhoes":
        return x / 1e3 if med >= 1e5 else x
    return x


def december_or_last(s: pd.Series):
    s = s.sort_index()
    dez = s[s.index.month == 12]
    return dez.iloc[-1] if not dez.empty else (s.iloc[-1] if len(s) else np.nan)


def annualize(df: pd.DataFrame, agg_map: dict, default="mean") -> pd.DataFrame:
    df = df.copy()
    df.index = pd.to_datetime(df.index)
    grouped = df.groupby(df.index.year)
    out = {}
    for col in df.columns:
        func = agg_map.get(col)
        if func is None:
            func = pd.Series.mean if default == "mean" else (lambda s: s.iloc[-1])
        out[col] = grouped[col].apply(func)
    res = pd.DataFrame(out)
    res.index.name = "Ano"
    return res


def to_col_data(df: pd.DataFrame, date_col_name="Data") -> pd.DataFrame:
    d = df.copy().reset_index()
    d = d.rename(columns={d.columns[0]: date_col_name})
    return d


def ajustar_largura_colunas(ws, df: pd.DataFrame):
    for idx, col in enumerate(df.columns, start=1):
        col_str = str(col)
        max_len = max([len(col_str)] + [len(str(v)) for v in df[col].head(1000).fillna("")])
        ws.column_dimensions[ws.cell(row=1, column=idx).column_letter].width = min(max_len + 2, 30)


def extrai_dpf_dez(
    arquivo: str,
    aba: str,
    mes_alvo: str,
    ano_inicio: int,
    pt_meses: dict,
) -> pd.DataFrame:
    xl = pd.ExcelFile(arquivo, engine="openpyxl")
    df_raw = pd.read_excel(xl, sheet_name=aba, header=None)

    header_row_idx = None
    for i in range(len(df_raw)):
        row = df_raw.iloc[i]
        matches = sum(isinstance(v, str) and bool(PAT_MES_ANO.match(v.strip())) for v in row)
        if matches >= 10:
            header_row_idx = i
            break

    if header_row_idx is None:
        raise RuntimeError("Não localizei linha de cabeçalho com meses (ex.: 'Jan/01').")

    header = df_raw.iloc[header_row_idx].tolist()

    first_month_col = None
    for j in range(1, len(header)):
        v = header[j]
        if isinstance(v, str) and PAT_MES_ANO.match(v.strip()):
            first_month_col = j
            break

    if first_month_col is None:
        raise RuntimeError("Não encontrei a primeira coluna de mês após a coluna de rótulos.")

    periods = []
    for v in header[first_month_col:]:
        if isinstance(v, str) and PAT_MES_ANO.match(v.strip()):
            mon_abbr, yy = v.split("/")
            mm = pt_meses[mon_abbr]
            year = 2000 + int(yy)
            periods.append(pd.Timestamp(year=year, month=mm, day=1))
        else:
            periods.append(pd.NaT)

    def find_row_index(prefixes):
        for i in range(len(df_raw)):
            c0 = df_raw.iloc[i, 0]
            if isinstance(c0, str):
                c0s = c0.strip()
                if any(c0s.startswith(p) for p in prefixes):
                    return i
        return None

    def extract_series(row_idx):
        values = df_raw.iloc[row_idx, first_month_col:first_month_col + len(periods)].tolist()
        s = pd.Series(values, index=periods)
        return pd.to_numeric(s, errors="coerce")

    idx_dpf = find_row_index(["DPF EM PODER DO PÚBLICO"])
    idx_dpmfi = find_row_index(["DPMFi"])
    idx_dpfe = find_row_index(["DPFe", "DPFe "])

    if idx_dpf is None or idx_dpmfi is None or idx_dpfe is None:
        raise RuntimeError(
            f"Não encontrei todas as linhas: DPF={idx_dpf}, DPMFi={idx_dpmfi}, DPFe={idx_dpfe}"
        )

    s_dpf = extract_series(idx_dpf)
    s_dpmfi = extract_series(idx_dpmfi)
    s_dpfe = extract_series(idx_dpfe)

    valid_periods = pd.DatetimeIndex([p for p in s_dpf.index if isinstance(p, pd.Timestamp)])
    filtro = (valid_periods.month == pt_meses[mes_alvo]) & (valid_periods.year >= ano_inicio)
    dezembros = valid_periods[filtro]

    out = pd.DataFrame(
        {
            "Ano": dezembros.year,
            "DPF": s_dpf.reindex(dezembros).values,
            "DPMFi": s_dpmfi.reindex(dezembros).values,
            "DPFe": s_dpfe.reindex(dezembros).values,
        }
    ).sort_values("Ano").reset_index(drop=True)

    return out