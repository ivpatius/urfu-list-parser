#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
UrFU master's priority allocator
- Pulls applicants from https://urfu.ru/ru/alpha/full/ (HTML tables)
- Pulls budget quotas from https://urfu.ru/ru/ratings-masters/ (HTML lists/tables)
- Filters: master's level + budget financing + consent submitted
- Allocates by applicant priorities into programs with quotas
- Outputs a single Excel: priority_allocation.xlsx
Dependencies: requests, pandas, lxml, openpyxl
"""
import re
import sys
from typing import List, Optional, Tuple, Dict
import requests
import pandas as pd
from collections import defaultdict

URL_APPLICANTS = "https://urfu.ru/ru/alpha/full/"
URL_QUOTAS = "https://urfu.ru/ru/ratings-masters/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/127.0.0.0 Safari/537.36"
}

def fetch_html(url: str) -> str:
    r = requests.get(url, headers=HEADERS, timeout=60)
    if not r.encoding or r.encoding.lower() in ("iso-8859-1", "latin-1"):
        r.encoding = r.apparent_encoding or "utf-8"
    return r.text

def read_all_tables(html: str) -> List[pd.DataFrame]:
    try:
        tables = pd.read_html(html, flavor="lxml")
    except ValueError:
        return []
    good = []
    for t in tables:
        if isinstance(t, pd.DataFrame) and t.shape[1] >= 3 and t.shape[0] > 0:
            good.append(t)
    return good

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
    html = fetch_html(URL_APPLICANTS)
    tables = read_all_tables(html)
    if not tables:
        raise RuntimeError("Таблицы с абитуриентами не найдены на странице alpha/full.")
    df = pd.concat([normalize_columns(t) for t in tables], ignore_index=True, sort=False)

    # Locate columns
    col_level = find_col(df, [r"уровень", r"уровень\s+образования", r"уровень\s+подготовки"])
    col_institute = find_col(df, [r"институт", r"институт/факультет", r"подразделение", r"факультет"])
    col_program = find_col(df, [r"направ", r"специальност", r"программа", r"profile|major"])
    col_name = find_col(df, [r"фио", r"фамилия", r"имя", r"отчество", r"name"])
    col_basis = find_col(df, [r"осн\w*\s*обуч", r"источник\s*фин", r"форма\s*фин", r"осн\w* зачисл", r"бюджет"])
    col_consent = find_col(df, [r"соглас\w*\s*на\s*зачислен", r"согласие", r"consent"])
    col_priority = find_col(df, [r"приорит", r"приоритет\s*заявления", r"очередность", r"выбор"])
    col_score_sum = find_col(df, [r"сумм\w*\s+конкурсн\w*\s+балл", r"сумм\w*\s+балл", r"конкурсн\w*\s+балл"])
    col_exam = find_col(df, [r"балл\w*\s+за\s+ВИ", r"экзамен", r"ВИ"])

    # Filter master's
    if col_level:
        df = df[df[col_level].astype(str).str.contains("магистр", case=False, na=False)]

    # Budget + consent
    if col_basis:
        df = df[is_budget_series(df[col_basis])]
    if col_consent:
        df = df[has_consent_series(df[col_consent])]

    # Clean & standardize columns
    df = df.copy()
    if col_priority:
        df["Приоритет"] = df[col_priority].apply(extract_number_safe)
    else:
        # if no explicit priority, assume 1
        df["Приоритет"] = 1

    df["Институт"] = df[col_institute] if col_institute else "Н/Д"
    df["Программа"] = df[col_program] if col_program else "Н/Д"
    df["ФИО"] = df[col_name] if col_name else ""
    if col_score_sum:
        df["Сумма баллов"] = df[col_score_sum].apply(extract_number_safe)
    else:
        df["Сумма баллов"] = None
    if col_exam:
        df["Баллы ВИ"] = df[col_exam].apply(extract_number_safe)
    else:
        df["Баллы ВИ"] = None

    # Drop rows lacking program or institute
    df = df[~df["Программа"].isna() & ~df["Институт"].isna()].copy()
    # Priority must be >=1
    df["Приоритет"] = df["Приоритет"].fillna(1).astype(int)
    return df

def get_quotas() -> pd.DataFrame:
    """
    Try to parse quotas (budget places) from ratings-masters page.
    If fails, raise informative error instructing to provide CSV.
    """
    html = fetch_html(URL_QUOTAS)
    # First, try tables:
    tables = read_all_tables(html)
    frames = []
    for t in tables:
        t = normalize_columns(t)
        # Heuristic: find columns containing program and budget seats
        col_inst = find_col(t, [r"институт|филиал|подразделение|факультет"])
        col_prog = find_col(t, [r"программа|направ", r"специальност"])
        col_budget = find_col(t, [r"бюджетн\w*\s*мест", r"план\s*приема|план\s*набора|количеств\w*\s*мест"])
        if col_prog and col_budget:
            dfq = pd.DataFrame({
                "Институт": t[col_inst] if col_inst else "Н/Д",
                "Программа": t[col_prog],
                "Бюджетных мест": t[col_budget].apply(lambda x: int(extract_number_safe(x) or 0))
            })
            frames.append(dfq)
    if frames:
        q = pd.concat(frames, ignore_index=True).dropna(subset=["Программа"])
        # Clean program names
        q["Программа"] = q["Программа"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        q["Институт"] = q["Институт"].astype(str).str.replace(r"\s+", " ", regex=True).str.strip()
        # Keep positive quotas
        q = q[q["Бюджетных мест"] > 0]
        return q

    # As fallback, try to harvest key-value rows via regex from HTML (e.g., "Программа ... Бюджетных мест: 12")
    items = []
    # naive blocks split
    for block in re.split(r"</?(?:tr|div|li)[^>]*>", html, flags=re.I):
        if not block:
            continue
        # find a program-ish phrase
        prog_m = re.search(r"(?:Направление|Программа|Специальность)[^<:]*[:\-]?\s*([^\n<]{5,120})", block, flags=re.I)
        budget_m = re.search(r"(?:Бюджетн\w*\s*мест|План[^<]*):?\s*(\d+)", block, flags=re.I)
        inst_m = re.search(r"(?:Институт|Филиал|Подразделение)[^<:]*[:\-]?\s*([^\n<]{3,120})", block, flags=re.I)
        if prog_m and budget_m:
            items.append({
                "Институт": inst_m.group(1).strip() if inst_m else "Н/Д",
                "Программа": prog_m.group(1).strip(),
                "Бюджетных мест": int(budget_m.group(1))
            })
    if items:
        return pd.DataFrame(items)

    raise RuntimeError("Не удалось автоматически извлечь квоты с ratings-masters. "
                       "Сохраните CSV с колонками: Институт, Программа, Бюджетных мест — "
                       "и передайте путь через параметр --quotas path.csv")

def normalize_key(s: str) -> str:
    return re.sub(r"\s+", " ", str(s)).strip().lower()

def allocate(df_app: pd.DataFrame, df_q: pd.DataFrame) -> pd.DataFrame:
    """
    Deferred acceptance style: iterate priorities in ascending order.
    Within a program, keep applicants up to quota with highest scores.
    Tie-breakers: higher 'Сумма баллов', then higher 'Баллы ВИ', then lexicographic ФИО.
    Returns dataframe of admitted with extra columns: 'Назначенная программа', 'Приоритет назнач.'
    """
    # Build quota dict
    quota = {}
    for _, r in df_q.iterrows():
        k = (normalize_key(r["Институт"]), normalize_key(r["Программа"]))
        quota[k] = int(r["Бюджетных мест"])

    # Prepare applicants sorted by score desc, then exam, then name
    def sort_key(row):
        return (
            -(row.get("Сумма баллов") if pd.notna(row.get("Сумма баллов")) else -1e9),
            -(row.get("Баллы ВИ") if pd.notna(row.get("Баллы ВИ")) else -1e9),
            str(row.get("ФИО") or "")
        )

    # Group applicants by priority
    priorities = sorted(df_app["Приоритет"].unique())
    placed = []  # records
    remaining = df_app.copy()

    # we'll assume each row corresponds to the application for a specific program (already filtered to that program)
    # if dataset has only one program per row, priorities may require multiple rows per person; if not, we treat as single choice
    # For practical purposes, we perform per-priority round:
    for pr in sorted(priorities):
        round_df = remaining[remaining["Приоритет"] == pr].copy()
        # Sort by score desc etc
        round_df = round_df.sort_values(by=["Сумма баллов", "Баллы ВИ", "ФИО"], ascending=[False, False, True], kind="mergesort")
        # Try to seat into their stated program
        accepted_idx = []
        for idx, row in round_df.iterrows():
            key = (normalize_key(row["Институт"]), normalize_key(row["Программа"]))
            cap = quota.get(key, 0)
            if cap > 0:
                # accept
                accepted_idx.append(idx)
                quota[key] = cap - 1
        accepted = round_df.loc[accepted_idx]
        if not accepted.empty:
            acc = accepted.copy()
            acc["Назначенная программа"] = acc["Программа"]
            acc["Приоритет назнач."] = pr
            placed.append(acc)
            # Remove accepted from remaining
            remaining = remaining.drop(index=accepted.index)

    if placed:
        result = pd.concat(placed, ignore_index=True)
    else:
        result = pd.DataFrame(columns=list(df_app.columns) + ["Назначенная программа", "Приоритет назнач."])
    return result

def build_output_excel(df_adm: pd.DataFrame, df_q: pd.DataFrame, out_path: str = "priority_allocation.xlsx"):
    # Excel per institute, with sections per program
    with pd.ExcelWriter(out_path) as writer:
        for inst, sub in df_adm.groupby("Институт"):
            # Within sheet, we write programs sequentially separated by blank rows
            sheet_df_list = []
            for prog, grp in sub.groupby("Назначенная программа"):
                header = pd.DataFrame({"Институт": [inst], "Назначенная программа": [prog], "Примечание": [f"Бюджет, согласие, приоритет"]})
                cols = ["ФИО", "Сумма баллов", "Баллы ВИ", "Приоритет назнач."]
                view = grp.sort_values(by=["Сумма баллов", "Баллы ВИ", "ФИО"], ascending=[False, False, True])[cols]
                block = pd.concat([header, view], ignore_index=True)
                sheet_df_list.append(block)
                # add empty row
                sheet_df_list.append(pd.DataFrame({"Институт": [""], "Назначенная программа": [""], "Примечание": [""]}))
            if sheet_df_list:
                sheet_df = pd.concat(sheet_df_list, ignore_index=True, sort=False)
            else:
                sheet_df = pd.DataFrame({"Сообщение": ["Нет зачисленных по критериям."]})
            # Excel sheet name limited to 31
            sheet_name = str(inst)[:31] if pd.notna(inst) else "НД"
            sheet_df.to_excel(writer, sheet_name=sheet_name if sheet_name else "НД", index=False)

def main(argv: List[str]) -> int:
    import argparse
    p = argparse.ArgumentParser(description="UrFU master's priority allocator")
    p.add_argument("--quotas", help="Путь к CSV с колонками: Институт, Программа, Бюджетных мест (если не удастся спарсить с сайта)")
    p.add_argument("--out", default="priority_allocation.xlsx", help="Имя выходного Excel")
    args = p.parse_args(argv)

    print("Загружаю абитуриентов (alpha/full)...")
    df_app = get_applicants()

    print("Загружаю квоты (ratings-masters)...")
    if args.quotas:
        df_q = pd.read_csv(args.quotas)
        # Normalize columns
        ren = {}
        for c in df_q.columns:
            lc = c.lower()
            if "институт" in lc or "филиал" in lc:
                ren[c] = "Институт"
            elif "програм" in lc or "направ" in lc or "специал" in lc:
                ren[c] = "Программа"
            elif "мест" in lc or "план" in lc or "квот" in lc:
                ren[c] = "Бюджетных мест"
        df_q = df_q.rename(columns=ren)
        df_q["Бюджетных мест"] = df_q["Бюджетных мест"].apply(lambda x: int(extract_number_safe(x) or 0))
    else:
        try:
            df_q = get_quotas()
        except Exception as e:
            print("\nВнимание: не удалось автоматически получить квоты с сайта.")
            print("Ошибка:", e)
            print("Создайте CSV с колонками: Институт, Программа, Бюджетных мест — и укажите --quotas путь.csv")
            return 2

    print("Выполняю распределение по приоритетам...")
    df_adm = allocate(df_app, df_q)

    print("Сохраняю Excel...")
    build_output_excel(df_adm, df_q, args.out)
    print(f"Готово: {args.out}")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
