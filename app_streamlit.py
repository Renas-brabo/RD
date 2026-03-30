from __future__ import annotations

import logging
import re
import shutil
import tempfile
import unicodedata
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
import streamlit as st
from bs4 import BeautifulSoup

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


# =========================================================
# Logging para interface
# =========================================================
class StreamlitLogHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.messages = []

    def emit(self, record):
        msg = self.format(record)
        self.messages.append(msg)


# =========================================================
# Utilidades gerais
# =========================================================
def normalize_text(text: str) -> str:
    text = str(text).strip().lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text


def current_file_signature(path: Path) -> str:
    stat = path.stat()
    return f"{path.resolve()}|{int(stat.st_mtime)}|{stat.st_size}"


def prepare_preview_df(df: pd.DataFrame, max_rows: int = 50) -> pd.DataFrame:
    """
    Mostra automaticamente as linhas mais recentes.
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
        df_view = (
            df_view.sort_values(date_col, ascending=False)
            .head(max_rows)
            .reset_index(drop=True)
        )
    elif year_col:
        df_view[year_col] = pd.to_numeric(df_view[year_col], errors="coerce")
        df_view = (
            df_view.sort_values(year_col, ascending=False)
            .head(max_rows)
            .reset_index(drop=True)
        )
    else:
        df_view = df_view.tail(max_rows).reset_index(drop=True)

    return df_view


# =========================================================
# Descoberta LOCAL do RMD
# =========================================================
def month_name_to_number(token: str) -> int | None:
    token = normalize_text(token)

    month_map = {
        "jan": 1,
        "janeiro": 1,
        "fev": 2,
        "fevereiro": 2,
        "mar": 3,
        "marco": 3,
        "março": 3,
        "abr": 4,
        "abril": 4,
        "mai": 5,
        "maio": 5,
        "jun": 6,
        "junho": 6,
        "jul": 7,
        "julho": 7,
        "ago": 8,
        "agosto": 8,
        "set": 9,
        "setembro": 9,
        "out": 10,
        "outubro": 10,
        "nov": 11,
        "novembro": 11,
        "dez": 12,
        "dezembro": 12,
    }

    return month_map.get(token)


def is_excel_temp_file(path: Path) -> bool:
    return path.name.startswith("~$")


def is_hidden_file(path: Path) -> bool:
    return path.name.startswith(".")


def looks_like_rmd_file(path: Path) -> bool:
    if not path.is_file():
        return False
    if path.suffix.lower() != ".xlsx":
        return False
    if is_excel_temp_file(path):
        return False
    if is_hidden_file(path):
        return False

    name = normalize_text(path.stem)
    keywords = ["rmd", "anexo_rmd", "anexo-rmd", "anexo rmd", "divida", "dpf"]
    return any(k in name for k in keywords)


def parse_rmd_month_year_from_name(file_path: Path) -> tuple[int, int] | None:
    """
    Tenta extrair (ano, mês) do nome do arquivo.
    Exemplos:
      - Anexo_RMD_Janeiro_26.xlsx
      - Anexo-RMD-Fev-2026.xlsx
      - RMD mar 25.xlsx
      - anexo.rmd.dez.2023.xlsx
    """
    stem = normalize_text(file_path.stem)
    tokens = [t for t in re.split(r"[_\-\s\.]+", stem) if t]

    month_num = None
    year_num = None

    for token in tokens:
        if month_num is None:
            maybe_month = month_name_to_number(token)
            if maybe_month is not None:
                month_num = maybe_month
                continue

        if year_num is None and re.fullmatch(r"\d{2}|\d{4}", token):
            y = int(token)
            year_num = 2000 + y if y < 100 else y

    if month_num is not None and year_num is not None:
        return year_num, month_num

    month_regex = (
        r"(jan(?:eiro)?|fev(?:ereiro)?|mar(?:co|ço)?|abr(?:il)?|mai(?:o)?|"
        r"jun(?:ho)?|jul(?:ho)?|ago(?:sto)?|set(?:embro)?|out(?:ubro)?|"
        r"nov(?:embro)?|dez(?:embro)?)"
    )
    year_regex = r"(\d{2}|\d{4})"

    match = re.search(month_regex + r".*?" + year_regex, stem)
    if not match:
        match = re.search(year_regex + r".*?" + month_regex, stem)

    if match:
        parts = match.groups()
        month_token = None
        year_token = None

        for part in parts:
            if re.fullmatch(r"\d{2}|\d{4}", part):
                year_token = part
            else:
                month_token = part

        if month_token and year_token:
            m = month_name_to_number(month_token)
            y = int(year_token)
            y = 2000 + y if y < 100 else y
            if m is not None:
                return y, m

    return None


def build_local_rmd_rank(path: Path) -> tuple:
    parsed = parse_rmd_month_year_from_name(path)
    mtime = path.stat().st_mtime
    normalized_name = normalize_text(path.name)

    if parsed is not None:
        year_num, month_num = parsed
        return (2, year_num, month_num, mtime, normalized_name)

    return (1, 0, 0, mtime, normalized_name)


def get_rmd_search_dir_from_config(cfg: dict) -> Path:
    configured = str(cfg.get("ARQUIVO_RMD", "")).strip()

    if not configured:
        return Path("rmd")

    p = Path(configured)

    if p.suffix:
        parent = p.parent
        return parent if str(parent) not in ("", ".") else Path("rmd")

    return p


def find_latest_local_rmd_file(rmd_dir: str | Path = "rmd") -> Path:
    rmd_dir = Path(rmd_dir)

    if not rmd_dir.exists():
        raise FileNotFoundError(f"Pasta de RMD não encontrada: {rmd_dir}")

    candidates = [p for p in rmd_dir.rglob("*.xlsx") if looks_like_rmd_file(p)]
    if not candidates:
        raise FileNotFoundError(
            f"Nenhum arquivo RMD válido (.xlsx) foi encontrado em: {rmd_dir}"
        )

    ranked = [(build_local_rmd_rank(p), p) for p in candidates]
    ranked.sort(key=lambda x: x[0], reverse=True)
    return ranked[0][1]


# =========================================================
# Descoberta WEB do RMD
# =========================================================
def build_rmd_page_url(year: int, month: int) -> str:
    return (
        "https://www.tesourotransparente.gov.br/publicacoes/"
        f"relatorio-mensal-da-divida-rmd/{year}/{month}"
    )


def build_rmd_base_url() -> str:
    return "https://www.tesourotransparente.gov.br/publicacoes/relatorio-mensal-da-divida-rmd/"


def month_number_to_pt_name(month: int) -> str:
    mapping = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Março",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    return mapping[month]


def month_number_to_pt_name_ascii(month: int) -> str:
    mapping = {
        1: "Janeiro",
        2: "Fevereiro",
        3: "Marco",
        4: "Abril",
        5: "Maio",
        6: "Junho",
        7: "Julho",
        8: "Agosto",
        9: "Setembro",
        10: "Outubro",
        11: "Novembro",
        12: "Dezembro",
    }
    return mapping[month]


def iter_recent_year_months(max_lookback_months: int = 18):
    """
    Gera pares (ano, mês) do mês atual para trás.
    """
    today = date.today()
    y, m = today.year, today.month

    for _ in range(max_lookback_months):
        yield y, m
        m -= 1
        if m == 0:
            m = 12
            y -= 1


def fetch_html(url: str, timeout: int = 60) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    return resp.text


def collect_link_candidates_from_html(page_url: str, html: str) -> list[dict]:
    """
    Coleta candidatos de link a partir de href e atributos alternativos.
    """
    soup = BeautifulSoup(html, "html.parser")
    candidates = []

    def add_candidate(raw_url: str, text: str, source_attr: str):
        if not raw_url:
            return

        full_url = urljoin(page_url, raw_url.strip())
        text = (text or "").strip()

        candidates.append(
            {
                "attachment_url": full_url,
                "anchor_text": text,
                "source_attr": source_attr,
            }
        )

    for a in soup.find_all("a"):
        text = a.get_text(" ", strip=True)

        for attr in ["href", "data-href", "data-url", "data-download", "download"]:
            raw = a.get(attr)
            if raw:
                add_candidate(raw, text, attr)

    for tag in soup.find_all(attrs={"data-href": True}):
        add_candidate(tag.get("data-href"), tag.get_text(" ", strip=True), "data-href")

    for tag in soup.find_all(attrs={"data-url": True}):
        add_candidate(tag.get("data-url"), tag.get_text(" ", strip=True), "data-url")

    return candidates


def score_attachment_candidate(
    candidate: dict,
    target_year: int | None = None,
    target_month: int | None = None,
) -> int:
    url_low = candidate["attachment_url"].lower()
    text_low = normalize_text(candidate.get("anchor_text", ""))

    score = 0

    if ".xlsx" in url_low:
        score += 8
    if ".zip" in url_low:
        score += 7
    if ".pdf" in url_low:
        score -= 5

    if "anexo" in url_low or "anexo" in text_low:
        score += 5
    if "rmd" in url_low or "rmd" in text_low:
        score += 5
    if "tabela" in url_low or "tabela" in text_low:
        score += 2

    if target_year is not None and target_month is not None:
        yy2 = str(target_year)[-2:]
        month_pt = normalize_text(month_number_to_pt_name(target_month))
        month_pt_ascii = normalize_text(month_number_to_pt_name_ascii(target_month))

        normalized_url = normalize_text(url_low)

        if str(target_year) in url_low or str(target_year) in text_low:
            score += 3
        if yy2 in url_low or yy2 in text_low:
            score += 2
        if month_pt in normalized_url or month_pt in text_low:
            score += 4
        if month_pt_ascii in normalized_url or month_pt_ascii in text_low:
            score += 4

    return score


def find_rmd_attachment_in_page(
    page_url: str,
    target_year: int | None = None,
    target_month: int | None = None,
) -> dict:
    html = fetch_html(page_url, timeout=60)
    candidates = collect_link_candidates_from_html(page_url, html)

    if not candidates:
        raise FileNotFoundError(
            f"Nenhum link candidato foi encontrado na página: {page_url}"
        )

    scored = []
    for c in candidates:
        score = score_attachment_candidate(
            c,
            target_year=target_year,
            target_month=target_month,
        )
        scored.append({**c, "score": score})

    scored = [c for c in scored if c["score"] > 0]

    if not scored:
        raise FileNotFoundError(
            f"Encontrei links na página, mas nenhum parece ser anexo do RMD: {page_url}"
        )

    scored.sort(key=lambda x: x["score"], reverse=True)
    best = scored[0]

    return {
        "page_url": page_url,
        "attachment_url": best["attachment_url"],
        "anchor_text": best["anchor_text"],
        "score": best["score"],
        "source_attr": best["source_attr"],
    }


def discover_latest_rmd_on_web(max_lookback_months: int = 18) -> dict:
    """
    Estratégia robusta:
    1) tenta a página mensal específica;
    2) se falhar, tenta a página-base do RMD;
    3) recua mês a mês.
    """
    errors = []

    for year, month in iter_recent_year_months(max_lookback_months=max_lookback_months):
        monthly_url = build_rmd_page_url(year, month)
        base_url = build_rmd_base_url()

        for candidate_page in [monthly_url, base_url]:
            try:
                found = find_rmd_attachment_in_page(
                    candidate_page,
                    target_year=year,
                    target_month=month,
                )
                return {
                    "source_type": "web",
                    "source_label": "Portal Tesouro Transparente",
                    "source_signature": (
                        f"web|{found['page_url']}|{found['attachment_url']}"
                    ),
                    "page_url": found["page_url"],
                    "attachment_url": found["attachment_url"],
                    "anchor_text": found["anchor_text"],
                    "reference_year": year,
                    "reference_month": month,
                    "score": found["score"],
                    "source_attr": found["source_attr"],
                }
            except Exception as exc:
                errors.append(f"{candidate_page} -> {exc}")

    raise FileNotFoundError(
        "Não foi possível localizar um anexo de RMD na web. "
        + " | ".join(errors[-10:])
    )


def download_file_to_temp(url: str, suffix: str | None = None) -> str:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
        "Accept": "*/*",
    }

    resp = requests.get(url, headers=headers, timeout=120, allow_redirects=True)
    resp.raise_for_status()

    if suffix is None:
        content_type = resp.headers.get("Content-Type", "").lower()
        final_url = resp.url.lower()

        if ".zip" in final_url or "zip" in content_type:
            suffix = ".zip"
        elif (
            ".xlsx" in final_url
            or "spreadsheetml" in content_type
            or "excel" in content_type
        ):
            suffix = ".xlsx"
        elif ".pdf" in final_url or "pdf" in content_type:
            suffix = ".pdf"
        else:
            suffix = ".bin"

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp.write(resp.content)
        return tmp.name


def extract_excel_from_zip(zip_path: str) -> tuple[str, str]:
    """
    Extrai o primeiro .xlsx relevante do ZIP.
    Retorna:
      (excel_path, temp_extract_dir)
    """
    extract_dir = tempfile.mkdtemp(prefix="rmd_zip_")

    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)

    excel_files = [p for p in Path(extract_dir).rglob("*.xlsx") if p.is_file()]
    if not excel_files:
        raise FileNotFoundError(
            f"Nenhum arquivo .xlsx foi encontrado dentro do ZIP: {zip_path}"
        )

    def rank_excel_inside_zip(path: Path):
        name = normalize_text(path.name)
        return (
            "rmd" in name,
            "anexo" in name,
            "tabela" in name,
            name,
        )

    excel_files.sort(key=rank_excel_inside_zip, reverse=True)
    return str(excel_files[0]), extract_dir


def materialize_rmd_excel(source_info: dict) -> tuple[str, list[str], list[str]]:
    """
    Converte a origem escolhida em um caminho local de Excel pronto para o pipeline.
    Retorna:
      excel_path, temp_files, temp_dirs
    """
    temp_files: list[str] = []
    temp_dirs: list[str] = []

    if source_info["source_type"] == "local":
        return source_info["local_path"], temp_files, temp_dirs

    attachment_url = source_info["attachment_url"]
    downloaded_path = download_file_to_temp(attachment_url)
    temp_files.append(downloaded_path)

    lower = downloaded_path.lower()

    if lower.endswith(".xlsx"):
        return downloaded_path, temp_files, temp_dirs

    if lower.endswith(".zip"):
        excel_path, extract_dir = extract_excel_from_zip(downloaded_path)
        temp_dirs.append(extract_dir)
        return excel_path, temp_files, temp_dirs

    raise ValueError(
        f"O arquivo baixado da web não é XLSX nem ZIP: {downloaded_path}"
    )


def discover_preferred_rmd_source(cfg: dict) -> dict:
    """
    Estratégia:
    1) tenta web;
    2) se falhar, usa o RMD local mais recente da pasta configurada.
    """
    local_dir = get_rmd_search_dir_from_config(cfg)

    try:
        return discover_latest_rmd_on_web(max_lookback_months=18)
    except Exception as web_exc:
        latest_local = find_latest_local_rmd_file(local_dir)
        return {
            "source_type": "local",
            "source_label": "Repositório local",
            "source_signature": f"local|{current_file_signature(latest_local)}",
            "local_path": str(latest_local),
            "fallback_reason": str(web_exc),
        }


# =========================================================
# RMD TABLE (integrado do script RD-Data-Public Debt Table.py)
# =========================================================
RMD_REFERENCE_SHEET = "2.1"
RMD_MONTH_SCAN_ROWS = 60

RMD_ROW_MAP = [
    {
        "key": "total_debt",
        "display": "Federal Public Debt (R$ bn)",
        "sheet": "2.1",
        "row_label": ["DPF EM PODER DO PUBLICO", "DPF EM PODER DO PÚBLICO", "DPF", "Divida Publica Federal"],
        "scale": 1.0,
        "layout": "periods_in_columns",
    },
    {
        "key": "domestic",
        "display": "Domestic",
        "sheet": "2.1",
        "row_label": ["DPMFi"],
        "scale": 1.0,
        "layout": "periods_in_columns",
    },
    {
        "key": "fixed_rate",
        "display": "Fixed-rate",
        "sheet": "2.5",
        "row_label": ["Prefixado", "Fixed-rate", "Fixed rate"],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
    {
        "key": "inflation_linked",
        "display": "Inflation-linked",
        "sheet": "2.5",
        "row_label": [
            "Indice de Precos",
            "Indice Precos",
            "Precos",
            "Índice de Preços",
            "Inflation-linked",
            "Price-indexed",
            "Price indexed",
        ],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
    {
        "key": "selic",
        "display": "Selic rate",
        "sheet": "2.5",
        "row_label": ["Taxa Flutuante", "Flutuante", "Floating-rate", "Floating rate", "Selic"],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
    {
        "key": "fx",
        "display": "FX",
        "sheet": "2.5",
        "row_label": ["Cambio", "Câmbio", "'Câmbio", "FX"],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
    {
        "key": "other",
        "display": "Other",
        "sheet": "2.5",
        "row_label": ["Demais", "Other"],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
    {
        "key": "external",
        "display": "External (R$ bn)",
        "sheet": "2.1",
        "row_label": ["DPFe"],
        "scale": 1.0,
        "layout": "periods_in_columns",
    },
    {
        "key": "avg_maturity",
        "display": "Average Maturity (years)",
        "sheet": "3.7",
        "row_label": ["DPF", "Divida Publica Federal"],
        "scale": 1.0,
        "layout": "periods_in_columns",
    },
    {
        "key": "maturing_12m_rs",
        "display": "Maturing in 12 months (R$ bn)",
        "sheet": "3.1",
        "row_label": ["Ate 12 meses", "Até 12 meses", "Maturing in 12 months"],
        "scale": 1.0,
        "layout": "periods_in_rows",
    },
]

RMD_PERCENT_AFTER = {
    "total_debt": ("total_debt", "total_debt"),
    "domestic": ("domestic", "total_debt"),
    "fixed_rate": ("fixed_rate", "total_debt"),
    "inflation_linked": ("inflation_linked", "total_debt"),
    "selic": ("selic", "total_debt"),
    "fx": ("fx", "total_debt"),
    "other": ("other", "total_debt"),
    "external": ("external", "total_debt"),
    "maturing_12m_rs": ("maturing_12m_rs", "total_debt"),
}

RMD_MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12
}

RMD_MESES_EN_CAP = {
    1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
    7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
}


def rmd_normalize_text(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = re.sub(r"[^a-z0-9]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def rmd_normalize_date_text(s):
    if s is None:
        return ""
    s = str(s).strip().lower()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.replace("-", "/")
    s = re.sub(r"[^a-z0-9/]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def rmd_month_token_to_datetime(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None

    if isinstance(x, (pd.Timestamp, datetime)):
        return datetime(x.year, x.month, 1)

    if isinstance(x, (int, float)) and not pd.isna(x):
        if 20000 <= float(x) <= 60000:
            base = datetime(1899, 12, 30)
            dt = base + timedelta(days=float(x))
            return datetime(dt.year, dt.month, 1)

    s = rmd_normalize_date_text(str(x))

    m = re.match(r"^(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)\s*/\s*(\d{2,4})$", s)
    if m:
        mes = RMD_MESES_PT[m.group(1)]
        ano_txt = m.group(2)
        ano = 2000 + int(ano_txt) if len(ano_txt) == 2 else int(ano_txt)
        return datetime(ano, mes, 1)

    m = re.match(r"^(\d{1,2})\s*/\s*(\d{2,4})$", s)
    if m:
        mes = int(m.group(1))
        ano_txt = m.group(2)
        if 1 <= mes <= 12:
            ano = 2000 + int(ano_txt) if len(ano_txt) == 2 else int(ano_txt)
            return datetime(ano, mes, 1)

    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d/%m/%y", "%m/%d/%y", "%Y/%m/%d"):
        try:
            dt = datetime.strptime(s, fmt)
            return datetime(dt.year, dt.month, 1)
        except Exception:
            pass

    return None


def rmd_dt_to_en_token(dt):
    return f"{RMD_MESES_EN_CAP[dt.month]}/{str(dt.year)[-2:]}"


def rmd_month_variants(dt):
    return {
        rmd_normalize_date_text(rmd_dt_to_en_token(dt)),
        rmd_normalize_date_text(f"{dt.month:02d}/{str(dt.year)[-2:]}"),
        rmd_normalize_date_text(f"{dt.month:02d}/{dt.year}"),
    }


def rmd_infer_reference_month_from_filename(file_path):
    nome = Path(file_path).stem
    nome_norm = rmd_normalize_text(nome)

    mapa_meses = {
        "janeiro": 1, "fevereiro": 2, "marco": 3, "abril": 4, "maio": 5, "junho": 6,
        "julho": 7, "agosto": 8, "setembro": 9, "outubro": 10, "novembro": 11, "dezembro": 12,
        "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
        "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
    }

    for mes_txt, mes_num in mapa_meses.items():
        m = re.search(rf"{mes_txt}[ _-]?(\d{{2,4}})", nome_norm)
        if m:
            ano_txt = m.group(1)
            ano = 2000 + int(ano_txt) if len(ano_txt) == 2 else int(ano_txt)
            return datetime(ano, mes_num, 1)

    return None


def rmd_load_sheet(path, sheet_name):
    return pd.read_excel(path, sheet_name=sheet_name, header=None, engine="openpyxl")


def rmd_find_month_header_general(df, top_n_rows=RMD_MONTH_SCAN_ROWS, min_required=3):
    best_row = None
    best_map = {}
    max_rows = min(top_n_rows, df.shape[0])

    for r in range(max_rows):
        current = {}
        for c in range(df.shape[1]):
            dt = rmd_month_token_to_datetime(df.iat[r, c])
            if dt is not None:
                current[dt] = c
        if len(current) > len(best_map) and len(current) >= min_required:
            best_map = current
            best_row = r

    if best_row is None or not best_map:
        raise ValueError("Não encontrei a linha de meses na aba.")

    return best_row, dict(sorted(best_map.items(), key=lambda x: x[0]))


def rmd_find_reference_months(df):
    return rmd_find_month_header_general(df, top_n_rows=RMD_MONTH_SCAN_ROWS, min_required=6)


def rmd_choose_periods(month_cols, file_path=None):
    meses_disponiveis = sorted(month_cols.keys())
    main = None

    if file_path is not None:
        main = rmd_infer_reference_month_from_filename(file_path)

    if main is None:
        main = meses_disponiveis[-1]

    if main not in meses_disponiveis:
        raise ValueError(
            f"O mês de referência {rmd_dt_to_en_token(main)} não foi encontrado nas colunas da aba '{RMD_REFERENCE_SHEET}'."
        )

    prev = datetime(main.year, main.month, 1) - pd.DateOffset(months=1)
    prev = datetime(prev.year, prev.month, 1)
    yoy = datetime(main.year - 1, main.month, 1)

    if prev not in meses_disponiveis:
        raise ValueError(
            f"Não encontrei o mês anterior {rmd_dt_to_en_token(prev)} na aba '{RMD_REFERENCE_SHEET}'."
        )
    if yoy not in meses_disponiveis:
        raise ValueError(
            f"Não encontrei o mesmo mês do ano anterior {rmd_dt_to_en_token(yoy)} na aba '{RMD_REFERENCE_SHEET}'."
        )

    return main, prev, yoy


def rmd_find_period_columns_in_sheet(df, periods, sheet_name):
    header_row, month_cols = rmd_find_month_header_general(df, top_n_rows=RMD_MONTH_SCAN_ROWS, min_required=3)
    missing = [rmd_dt_to_en_token(p) for p in periods if p not in month_cols]

    if missing:
        found = {}
        for r in range(min(RMD_MONTH_SCAN_ROWS, df.shape[0])):
            for c in range(df.shape[1]):
                cell_norm = rmd_normalize_date_text(df.iat[r, c])
                if not cell_norm:
                    continue
                for dt in periods:
                    if dt in found:
                        continue
                    if cell_norm in rmd_month_variants(dt):
                        found[dt] = c
        for p in periods:
            if p not in found and p in month_cols:
                found[p] = month_cols[p]

        missing = [rmd_dt_to_en_token(p) for p in periods if p not in found]
        if missing:
            raise ValueError(
                f"Não encontrei as colunas dos períodos {missing} na aba '{sheet_name}'."
            )
        return header_row, found

    return header_row, {p: month_cols[p] for p in periods}


def rmd_find_period_rows_in_sheet(df, periods, sheet_name):
    found = {}
    for r in range(df.shape[0]):
        for c in range(min(4, df.shape[1])):
            dt = rmd_month_token_to_datetime(df.iat[r, c])
            if dt is not None:
                for target in periods:
                    if target not in found and dt == target:
                        found[target] = r

    missing = [rmd_dt_to_en_token(p) for p in periods if p not in found]
    if missing:
        raise ValueError(
            f"Não encontrei as linhas dos períodos {missing} na aba '{sheet_name}'."
        )

    return found


def rmd_row_text(df, r, ncols=6):
    vals = []
    for c in range(min(ncols, df.shape[1])):
        v = df.iat[r, c]
        if pd.notna(v):
            vals.append(str(v))
    return rmd_normalize_text(" ".join(vals))


def rmd_find_row_by_label(df, target_label, min_row=0):
    if isinstance(target_label, (list, tuple, set)):
        targets = [rmd_normalize_text(x) for x in target_label]
    else:
        targets = [rmd_normalize_text(target_label)]

    for r in range(min_row, df.shape[0]):
        for c in range(min(4, df.shape[1])):
            cell = rmd_normalize_text(df.iat[r, c])
            if not cell:
                continue
            for t in targets:
                if cell == t:
                    return r

    for r in range(min_row, df.shape[0]):
        txt = rmd_row_text(df, r, ncols=6)
        if not txt or txt.startswith("anexo"):
            continue
        for t in targets:
            if txt == t:
                return r

    for r in range(min_row, df.shape[0]):
        txt = rmd_row_text(df, r, ncols=6)
        if not txt or txt.startswith("anexo"):
            continue
        for t in targets:
            if t in txt:
                return r

    raise ValueError(f"Não encontrei a linha '{target_label}'.")


def rmd_find_col_by_label(df, target_label, max_scan_rows=80):
    if isinstance(target_label, (list, tuple, set)):
        targets = [rmd_normalize_text(x) for x in target_label]
    else:
        targets = [rmd_normalize_text(target_label)]

    best_col = None
    best_score = 0

    for c in range(df.shape[1]):
        textos = []
        for r in range(min(max_scan_rows, df.shape[0])):
            v = df.iat[r, c]
            if pd.notna(v):
                textos.append(str(v))
        txt = rmd_normalize_text(" ".join(textos))
        if not txt:
            continue

        for t in targets:
            if txt == t or t in txt:
                return c

        txt_tokens = set(txt.split())
        for t in targets:
            t_tokens = set(t.split())
            score = len(txt_tokens.intersection(t_tokens))
            if score > best_score:
                best_score = score
                best_col = c

    if best_col is not None and best_score >= 1:
        return best_col

    raise ValueError(f"Não encontrei a coluna '{target_label}'.")


def rmd_extract_value(df, row_idx, col_idx, scale=1.0):
    v = df.iat[row_idx, col_idx]
    if pd.isna(v):
        return None
    try:
        return float(v) / scale
    except Exception:
        return None


def build_rmd_raw_table(path):
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path.resolve()}")

    df_ref = rmd_load_sheet(path, RMD_REFERENCE_SHEET)
    _, ref_month_cols = rmd_find_reference_months(df_ref)
    p1, p2, p3 = rmd_choose_periods(ref_month_cols, file_path=path)
    periods = [p1, p2, p3]
    period_labels = [rmd_dt_to_en_token(p) for p in periods]

    sheets_cache = {RMD_REFERENCE_SHEET: df_ref}
    values_by_key = {}

    for item in RMD_ROW_MAP:
        sheet = item["sheet"]
        if sheet not in sheets_cache:
            sheets_cache[sheet] = rmd_load_sheet(path, sheet)
        df = sheets_cache[sheet]

        layout = item.get("layout", "periods_in_columns")

        if layout == "periods_in_columns":
            header_row, period_cols = rmd_find_period_columns_in_sheet(df, periods, sheet)
            row_idx = rmd_find_row_by_label(df, item["row_label"], min_row=header_row + 1)
            vals = []
            for p in periods:
                col_idx = period_cols[p]
                vals.append(rmd_extract_value(df, row_idx, col_idx, scale=item.get("scale", 1.0)))
            values_by_key[item["key"]] = vals

        elif layout == "periods_in_rows":
            period_rows = rmd_find_period_rows_in_sheet(df, periods, sheet)
            col_idx = rmd_find_col_by_label(df, item["row_label"])
            vals = []
            for p in periods:
                row_idx = period_rows[p]
                vals.append(rmd_extract_value(df, row_idx, col_idx, scale=item.get("scale", 1.0)))
            values_by_key[item["key"]] = vals

        else:
            raise ValueError(f"Layout inválido em {item['key']}: {layout}")

    rows = []
    for item in RMD_ROW_MAP:
        vals = values_by_key[item["key"]]
        rows.append([item["key"], item["display"], *vals])

        if item["key"] in RMD_PERCENT_AFTER:
            ref_key, base_key = RMD_PERCENT_AFTER[item["key"]]
            ref_vals = values_by_key[ref_key]
            base_vals = values_by_key[base_key]

            pct_vals = []
            for a, b in zip(ref_vals, base_vals):
                if b in (None, 0) or pd.isna(b) or a is None or pd.isna(a):
                    pct_vals.append(None)
                else:
                    pct_vals.append(100 * a / b)

            rows.append([f"{item['key']}_pct", "%", *pct_vals])

    raw = pd.DataFrame(rows, columns=["key", "Item", *period_labels])
    return raw, period_labels


def build_rmd_presentation_table(raw, period_labels):
    block = {}
    i = 0

    while i < len(raw):
        key = raw.iloc[i]["key"]
        item = raw.iloc[i]["Item"]
        vals = [raw.iloc[i][p] for p in period_labels]
        pct_vals = None

        if i + 1 < len(raw):
            next_key = str(raw.iloc[i + 1]["key"])
            next_item = raw.iloc[i + 1]["Item"]
            if next_item == "%" and next_key.startswith(str(key)):
                pct_vals = [raw.iloc[i + 1][p] for p in period_labels]
                i += 1

        block[key] = {"label": item, "values": vals, "pct": pct_vals}
        i += 1

    def make_value_pct_row(label, value_key, use_pct=True):
        vals = block[value_key]["values"]
        pct = block[value_key]["pct"] if use_pct else [None, None, None]
        row = [label]
        for v, p in zip(vals, pct):
            row.extend([v, p])
        return row

    def make_value_only_row(label, values):
        row = [label]
        for v in values:
            row.extend([v, None])
        return row

    presentation_rows = []
    presentation_rows.append(make_value_pct_row("Federal Public Debt (R$ bn)", "total_debt", True))
    presentation_rows.append(make_value_pct_row("Domestic", "domestic", True))
    presentation_rows.append(make_value_pct_row("Fixed-rate", "fixed_rate", True))
    presentation_rows.append(make_value_pct_row("Inflation-linked", "inflation_linked", True))
    presentation_rows.append(make_value_pct_row("Selic rate", "selic", True))
    presentation_rows.append(make_value_pct_row("FX", "fx", True))
    presentation_rows.append(make_value_pct_row("Other", "other", True))
    presentation_rows.append(make_value_pct_row("External (R$ bn)", "external", True))
    presentation_rows.append(["Maturity Profile", None, None, None, None, None, None])
    presentation_rows.append(make_value_only_row(" Average Maturity (years)", block["avg_maturity"]["values"]))
    presentation_rows.append(make_value_only_row(" Maturing in 12 months (R$ bn)", block["maturing_12m_rs"]["values"]))
    presentation_rows.append(make_value_only_row(" Maturing in 12 months (%)", block["maturing_12m_rs"]["pct"]))

    df = pd.DataFrame(
        presentation_rows,
        columns=[
            "Item",
            f"{period_labels[0]}_value", f"{period_labels[0]}_pct",
            f"{period_labels[1]}_value", f"{period_labels[1]}_pct",
            f"{period_labels[2]}_value", f"{period_labels[2]}_pct",
        ],
    )
    return df


def build_rmd_table_for_app(rmd_excel_path: str) -> pd.DataFrame:
    raw, period_labels = build_rmd_raw_table(rmd_excel_path)
    return build_rmd_presentation_table(raw, period_labels)


# =========================================================
# Execução do pipeline
# =========================================================
def run_pipeline_auto(source_info: dict):
    cfg = get_config()
    temp_files: list[str] = []
    temp_dirs: list[str] = []
    logger = None
    st_handler = None

    try:
        excel_path, temp_files, temp_dirs = materialize_rmd_excel(source_info)

        cfg["ARQUIVO_RMD"] = excel_path
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

        logger.info("Iniciando execução automática pelo Streamlit.")
        logger.info("Fonte RMD selecionada: %s", source_info["source_label"])
        logger.info("ARQUIVO_RMD em uso: %s", cfg["ARQUIVO_RMD"])

        validate_config(cfg)

        raw = collect_data(cfg, logger)
        processed, warnings = process_data(raw, cfg, logger)

        export_tables = build_export_tables(processed, logger)

        # ---------------------------------------------------------
        # NOVA ABA: dados_dpf
        # ---------------------------------------------------------
        try:
            rmd_df = build_rmd_table_for_app(cfg["ARQUIVO_RMD"])
            logger.info("Tabela RMD integrada com sucesso à saída final.")
        except Exception as exc:
            logger.warning("Falha ao montar a aba 'rmd': %s", exc)
            rmd_df = pd.DataFrame(
                {
                    "status": ["erro"],
                    "detalhe": [str(exc)],
                }
            )

        export_tables["dados_dpf"] = rmd_df

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

        return {
            "success": True,
            "source_info": source_info,
            "source_signature": source_info["source_signature"],
            "export_tables": export_tables,
            "warnings": warnings,
            "summary": summary,
            "logs": st_handler.messages if st_handler else [],
            "excel_bytes": excel_bytes,
            "output_path": str(output_path),
        }

    except Exception as exc:
        if logger is not None:
            logger.exception("Falha na execução automática: %s", exc)

        return {
            "success": False,
            "source_info": source_info,
            "source_signature": source_info.get("source_signature"),
            "error": str(exc),
            "logs": st_handler.messages if st_handler else [],
        }

    finally:
        for f in temp_files:
            try:
                if f and Path(f).exists():
                    Path(f).unlink()
            except Exception:
                pass

        for d in temp_dirs:
            try:
                if d and Path(d).exists():
                    shutil.rmtree(d, ignore_errors=True)
            except Exception:
                pass


# =========================================================
# Interface
# =========================================================
st.set_page_config(
    page_title="RD Data Dashboard",
    page_icon="📊",
    layout="wide",
)

st.title("📊 RD Data Dashboard")
st.caption(
    "Execução automática do pipeline com busca web do RMD e, caso falhe, usa arquivo local"
)

default_cfg = get_config()

try:
    latest_source = discover_preferred_rmd_source(default_cfg)
    latest_signature = latest_source["source_signature"]

    should_run = (
        "rd_result" not in st.session_state
        or st.session_state.get("rd_result", {}).get("source_signature") != latest_signature
    )

    if should_run:
        with st.spinner("Localizando a fonte mais recente do RMD e executando o pipeline..."):
            st.session_state["rd_result"] = run_pipeline_auto(latest_source)

    result = st.session_state.get("rd_result")

    if result:
        source_info = result.get("source_info", {})

        if result["success"]:
            st.success("Pipeline executado com sucesso.")

            st.subheader("Fonte do RMD utilizada")
            st.write(f"**Origem:** {source_info.get('source_label', '-')}")
            st.write(f"**Tipo:** {source_info.get('source_type', '-')}")

            if source_info.get("source_type") == "web":
                st.markdown(f"**Página do RMD:** {source_info.get('page_url', '-')}")
                st.markdown(f"**Anexo localizado:** {source_info.get('attachment_url', '-')}")
                if source_info.get("anchor_text"):
                    st.write(f"**Texto do link do anexo:** {source_info['anchor_text']}")
                if source_info.get("score") is not None:
                    st.write(f"**Score do candidato selecionado:** {source_info['score']}")
                if source_info.get("source_attr"):
                    st.write(f"**Atributo HTML usado:** {source_info['source_attr']}")
            else:
                st.write(f"**Arquivo local:** {source_info.get('local_path', '-')}")
                if source_info.get("fallback_reason"):
                    with st.expander("Motivo do fallback local", expanded=False):
                        st.code(source_info["fallback_reason"])

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
            st.caption("Exibindo automaticamente até 50 linhas mais recentes por aba.")

            export_tables = result["export_tables"]
            tab_names = list(export_tables.keys())
            tabs = st.tabs(tab_names)

            for tab, sheet_name in zip(tabs, tab_names):
                with tab:
                    df = export_tables[sheet_name]
                    df_view = prepare_preview_df(df, max_rows=50)

                    st.write(f"**Aba:** {sheet_name}")
                    st.write(f"Linhas: {len(df)} | Colunas: {len(df.columns)}")
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
            st.error("A execução automática falhou.")
            st.code(result.get("error", "Erro não detalhado."))

            if result.get("logs"):
                with st.expander("Logs da execução", expanded=True):
                    for msg in result["logs"]:
                        st.text(msg)

except Exception as exc:
    st.error("Não foi possível inicializar o dashboard.")
    st.code(str(exc))
