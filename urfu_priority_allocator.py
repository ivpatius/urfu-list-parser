#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UrFU master's priority allocator (PDF quotas)
- Applicants: https://urfu.ru/ru/alpha/full/ (HTML)
- Quotas: PDF https://magister.urfu.ru/fileadmin/user_upload/site_15406/2025/admission/Plan_priema_2025_magistratura.pdf
- Filters: master's + budget + consent
- Allocation: by priorities into programs within budget quotas
- Output: priority_allocation.xlsx

Dependencies:
  requests, pandas, lxml, openpyxl, pdfplumber
"""
import re
import sys
import io
from typing import List, Optional, Dict, Tuple
import requests
import pandas as pd

URL_APPLICANTS = "https://urfu.ru/ru/alpha/full/"
URL_PDF = "https://magister.urfu.ru/fileadmin/user_upload/site_15406/2025/admission/Plan_priema_2025_magistratura.pdf"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/127.0.0.0 Safari/537.36"
}

def fetch(url: str) -> bytes:
    r = requests.get(url, headers=HEADERS, timeout=120)
    r.raise_for_status()
    return r.content

def read_all_tables_html(html: str) -> List[pd.DataFrame]:
    try:
        tables = pd.read_html(html, flavor="lxml")
    except ValueError:
        return []
    return [t for t in tables if isinstance(t, pd.DataFrame) and t.shape[1] >= 3 and t.shape[0] > 0]

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [" ".join([str(x) for x in col if str(x) != "nan"]).strip() for col in df.columns.values]
    df.columns = [re.sub(r"\s+", " ", str(c)).strip() for c in df.columns]
    return df

def find_col(df: pd.DataFrame, patterns: List[str]) -> Optional[str]:
    for p in patterns:
        rx = re.compile(p, flags=re.I)
        for c in df.columns:
            if rx.search(str(c)):
                return c
    return None

def extract_number_safe(x) -> Optional[float]:
    if pd.isna(x):
        return None
    s = str(x).replace(",", ".")
    m = re.search(r"[-+]?\d+(?:[.,]\d+)?", s)
    if m:
        try:
            return float(m.group(0).replace(",", "."))
        except Exception:
            return None
    return None

def is_budget_series(s: pd.Series) -> pd.Series:
    v = s.astype(str).str.lower().str.replace(r"\s+", " ", regex=True)
    return (
        v.str.contains("бюдж", na=False)
        | v.str.contains("квот", na=False)
        | v.str.contains("целев", na=False)
        | v.str.contains("госконтракт", na=False)
    )

def has_consent_series(s: pd.Series) -> pd.Series:
    v = s.astype(str).str.lower().str.strip()
    neg = v.eq("") | v.eq("нет") | v.eq("false") | v.eq("0") | v.str.contains("не подав", na=False)
    pos = (
        v.eq("да") | v.eq("есть") | v.eq("true") | v.eq("1")
        | v.str.contains("подан", na=False)
        | v.str.contains("принят", na=False)
        | v.str.contains("соглас", na=False)
        | v.str.contains(r"\d{2}\.\d{2}\.\d{4}", na=False)
        | v.str.contains(r"\d{4}-\d{2}-\d{2}", na=False)
        | v.str.contains("✓|✔|■|подпис", na=False)
    )
    return (~neg) & pos

def get_applicants() -> pd.DataFrame:
    r = requests.get(URL_APPLICANTS, headers=HEADERS, timeout=120)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "latin-1"):
        r.encoding = r.apparent_encoding or "utf-8"
    html = r.text
    tables = read_all_tables_html(html)
    if not tables:
        raise RuntimeError("Таблицы с абитуриентами не найдены на странице alpha/full.")
    df = pd.concat([normalize_columns(t) for t in tables], ignore_index=True, sort=False)

    # Locate columns
    col_level = find_col(df, [r"уровень", r"уровень\s+образования", r"уровень\s+подготовки"])
    col_institute = find_col(df, [r"институт", r"институт/факультет", r"подразделение", r"факультет"])
    col_program = find_col(df, [r"направ", r"специальност", r"программа", r"profile|major"])
    col_name = find_col(df, [r"фио", r"фамилия", r"имя", r"отчество", r"name"])
    col_basis = find_col(df, [r"осн\\w*\\s*обуч", r"источник\\s*фин", r"форма\\s*фин", r"осн\\w* зачисл", r"бюджет"])
    col_consent = find_col(df, [r"соглас\\w*\\s*на\\s*зачислен", r"согласие", r"consent"])
    col_priority = find_col(df, [r"приорит", r"приоритет\\s*заявления", r"очередность", r"выбор"])
    col_score_sum = find_col(df, [r"сумм\\w*\\s+конкурсн\\w*\\s+балл", r"сумм\\w*\\s+балл", r"конкурсн\\w*\\s+балл"])
    col_exam = find_col(df, [r"балл\\w*\\s+за\\s+ВИ", r"экзамен", r"ВИ"])

    # Masters only
    if col_level:
        df = df[df[col_level].astype(str).str.contains("магистр", case=False, na=False)]

    # Budget + consent
    if col_basis:
        df = df[is_budget_series(df[col_basis])]
    if col_consent:
        df = df[has_consent_series(df[col_consent])]

    df = df.copy()
    df["Приоритет"] = df[col_priority].apply(extract_number_safe).fillna(1).astype(int) if col_priority else 1
    df["Институт"] = df[col_institute] if col_institute else "Н/Д"
    df["Программа"] = df[col_program] if col_program else "Н/Д"
    df["ФИО"] = df[col_name] if col_name else ""
    df["Сумма баллов"] = df[col_score_sum].apply(extract_number_safe) if col_score_sum else None
    df["Баллы ВИ"] = df[col_exam].apply(extract_number_safe) if col_exam else None

    df = df[~df["Программа"].isna() & ~df["Институт"].isna()].copy()
    return df

def pdf_to_tables(pdf_bytes: bytes) -> List[pd.DataFrame]:
    import pdfplumber
    dfs = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            try:
                tables = page.extract_tables()
            except Exception:
                tables = None
            if not tables:
                try:
                    tables = page.extract_tables(table_settings={
                        "vertical_strategy": "lines",
                        "horizontal_strategy": "lines",
                        "snap_tolerance": 3,
                    })
                except Exception:
                    tables = None
            if tables:
                for tbl in tables:
                    df = pd.DataFrame(tbl)
                    df = df.dropna(axis=1, how="all")
                    if df.shape[0] > 1:
                        header = df.iloc[0].astype(str).str.strip().tolist()
                        if any("направ" in h.lower() or "программ" in h.lower() or "институт" in h.lower() or "кцп" in h.lower() for h in header):
                            df.columns = header
                            df = df.iloc[1:].reset_index(drop=True)
                    dfs.append(df)
    return dfs

def get_quotas_from_pdf(pdf_path: Optional[str] = None) -> pd.DataFrame:
    data = fetch(URL_PDF) if not pdf_path else open(pdf_path, "rb").read()
    tables = pdf_to_tables(data)
    frames = []
    for t in tables:
        t = normalize_columns(t)
        col_inst = find_col(t, [r"институт|факультет|подразделение|филиал"])
        col_prog = find_col(t, [r"направлен|программа|специальност"])
        budget_cols = [c for c in t.columns if re.search(r"(КЦП|бюджет|основн|квот|план\s*приема|контрольн\w*\sцифр)", str(c), flags=re.I)]
        if col_prog and budget_cols:
            tmp = pd.DataFrame({
                "Институт": t[col_inst] if col_inst else "Н/Д",
                "Программа": t[col_prog].astype(str),
            })
            for c in budget_cols:
                tmp[c] = t[c].apply(lambda x: int(extract_number_safe(x) or 0))
            tmp["Бюджетных мест"] = tmp[budget_cols].sum(axis=1)
            frames.append(tmp[["Институт", "Программа", "Бюджетных мест"]])
    if not frames:
        raise RuntimeError("Не удалось распознать таблицы квот в PDF.")
    q = pd.concat(frames, ignore_index=True)
    q["Программа"] = q["Программа"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    q["Институт"] = q["Институт"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
    q = q[q["Бюджетных мест"] > 0]
    q = q.groupby(["Институт", "Программа"], as_index=False)["Бюджетных мест"].max()
    return q

def normalize_key(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()

def allocate(df_app: pd.DataFrame, df_q: pd.DataFrame) -> pd.DataFrame:
    quota = {(normalize_key(r["Институт"]), normalize_key(r["Программа"])): int(r["Бюджетных мест"])
             for _, r in df_q.iterrows()}
    df_app = df_app.copy()
    df_app["Сумма баллов"] = df_app["Сумма баллов"].fillna(-1e9)
    df_app["Баллы ВИ"] = df_app["Баллы ВИ"].fillna(-1e9)

    placed_records = []
    remaining = df_app.copy()
    for pr in sorted(remaining["Приоритет"].unique()):
        round_df = remaining[remaining["Приоритет"] == pr].sort_values(
            by=["Сумма баллов", "Баллы ВИ", "ФИО"], ascending=[False, False, True], kind="mergesort"
        )
        accepted_idx = []
        for idx, row in round_df.iterrows():
            key = (normalize_key(row["Институт"]), normalize_key(row["Программа"]))
            cap = quota.get(key, 0)
            if cap > 0:
                accepted_idx.append(idx)
                quota[key] = cap - 1
        if accepted_idx:
            acc = round_df.loc[accepted_idx].copy()
            acc["Назначенная программа"] = acc["Программа"]
            acc["Приоритет назнач."] = pr
            placed_records.append(acc)
            remaining = remaining.drop(index=acc.index)

    if placed_records:
        return pd.concat(placed_records, ignore_index=True)
    return pd.DataFrame(columns=list(df_app.columns) + ["Назначенная программа", "Приоритет назнач."])

def build_output_excel(df_adm: pd.DataFrame, out_path: str = "priority_allocation.xlsx"):
    with pd.ExcelWriter(out_path) as writer:
        for inst, sub in df_adm.groupby("Институт"):
            blocks = []
            for prog, grp in sub.groupby("Назначенная программа"):
                header = pd.DataFrame({"Институт": [inst], "Назначенная программа": [prog], "Примечание": ["Бюджет, согласие, приоритет"]})
                view = grp.sort_values(by=["Сумма баллов", "Баллы ВИ", "ФИО"], ascending=[False, False, True])[["ФИО", "Сумма баллов", "Баллы ВИ", "Приоритет назнач."]]
                blocks.append(pd.concat([header, view], ignore_index=True))
                blocks.append(pd.DataFrame({"Институт": [""], "Назначенная программа": [""], "Примечание": [""]}))
            sheet_df = pd.concat(blocks, ignore_index=True) if blocks else pd.DataFrame({"Сообщение": ["Нет зачисленных по критериям."]})
            sheet_name = str(inst)[:31] if pd.notna(inst) else "НД"
            sheet_df.to_excel(writer, sheet_name=sheet_name if sheet_name else "НД", index=False)

def main(argv: List[str]) -> int:
    import argparse
    p = argparse.ArgumentParser(description="UrFU master's priority allocator (PDF quotas)")
    p.add_argument("--pdf", help="Локальный путь к PDF с планом приема (если URL недоступен)")
    p.add_argument("--out", default="priority_allocation.xlsx", help="Имя выходного Excel")
    args = p.parse_args(argv)

    print("Загружаю абитуриентов (alpha/full)...")
    df_app = get_applicants()

    print("Извлекаю квоты из PDF...")
    try:
        df_q = get_quotas_from_pdf(args.pdf)
    except Exception as e:
        print("Ошибка разбора PDF:", e)
        print("Скачайте PDF и укажите путь через --pdf Plan_priema_2025_magistratura.pdf")
        return 2

    print("Распределяю по приоритетам...")
    df_adm = allocate(df_app, df_q)

    print("Сохраняю Excel...")
    build_output_excel(df_adm, args.out)
    print(f"Готово: {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
