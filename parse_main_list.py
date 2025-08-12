#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
import sys
import re
import requests
import pandas as pd
from pandas import json_normalize
from urllib.parse import urlencode
from statistics import mean
from tqdm.auto import tqdm

# ---------- Константы ----------
BASE_URL = "https://urfu.ru/api/entrant/"
PAGE_SIZE = 100  # важно: API любит ровно 100
MAX_RETRIES = 5
RETRY_BACKOFF = 2.0
TIMEOUT = 30
SLEEP_BETWEEN = 0.10  # вежливая пауза между запросами

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; EntrantScraper/1.5)",
    "Accept": "application/json",
}

# ---------- Сетевые утилиты ----------

def get_json_page(page: int):
    """
    GET page с повторами. Возвращает (payload, dt).
    Если пришёл 400/404 — возвращаем (None, dt) как сигнал закончить перебор.
    """
    params = {"page": page, "size": PAGE_SIZE}
    delay = 1.0
    last_dt = 0.0

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            t0 = time.perf_counter()
            resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
            last_dt = time.perf_counter() - t0

            if resp.status_code in (400, 404):
                # страница не существует/некорректная — конец
                return None, last_dt

            if resp.status_code in (429, 500, 502, 503, 504):
                ra = resp.headers.get("Retry-After")
                wait = float(ra) if ra and ra.isdigit() else delay
                time.sleep(wait)
                delay = min(delay * RETRY_BACKOFF, 30)
                continue

            resp.raise_for_status()
            return resp.json(), last_dt
        except requests.RequestException:
            if attempt == MAX_RETRIES:
                raise
            time.sleep(delay)
            delay = min(delay * RETRY_BACKOFF, 30)
    return None, last_dt

def extract_rows(payload):
    """
    Универсально достаёт список записей из ответа.
    Поддерживает: [ ... ] или {"results":[...]} или {"items":[...]} и т.п.
    """
    if payload is None:
        return []

    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        if isinstance(payload.get("results"), list):
            return payload["results"]
        for key in ("items", "data", "content", "rows", "entities"):
            if isinstance(payload.get(key), list):
                return payload[key]
        # fallback: первый list в словаре
        for v in payload.values():
            if isinstance(v, list):
                return v
    return []

def iterate_all_pages():
    """
    Жёсткий перебор страниц:
      /?page=1&size=100, /?page=2&size=100, ...
    Останавливаемся, когда:
      - payload == None (400/404),
      - список rows пустой.
    """
    all_rows = []
    timings = []
    pbar = tqdm(total=0, desc="Страницы (динамика)", unit="page")

    page = 1
    while True:
        payload, dt = get_json_page(page)
        timings.append(dt)
        avg_dt = mean(timings[-10:]) if timings else 0.0

        # динамически увеличиваем total, чтобы pbar считался корректно
        pbar.total = page
        pbar.update(1 - pbar.n)  # выставить прогресс на текущую страницу
        pbar.set_postfix(page=page, avg=f"{avg_dt:.2f}s")

        if payload is None:
            # 400/404 — дальше страниц нет
            break

        rows = extract_rows(payload)
        if not rows:
            # пустой ответ — конец
            break

        all_rows.extend(rows)
        page += 1
        time.sleep(SLEEP_BETWEEN)

    pbar.close()
    return all_rows

# ---------- Детекторы для фильтрации ----------

RX_MASTER = re.compile(r"\b(магист|магистр|magistr|master)\b", re.I)
RX_FULLTIME = re.compile(r"\b(очная|очно|full[-\s]*time)\b", re.I)
RX_PAID = re.compile(r"(платн|договор|контракт|коммерческ|внебюджет|paid|fee|tuition)", re.I)
RX_EGE = re.compile(r"\bЕГЭ\b", re.I)

def detect_master(row):
    # ищем признаки магистратуры в релевантных колонках
    texts = []
    for col in row.index:
        low = col.lower()
        if any(k in low for k in ("level", "edu", "degree", "program", "education",
                                  "direction", "spec", "уров", "программа", "направ")):
            texts.append(str(row[col]))
    return bool(RX_MASTER.search(" | ".join(texts)))

def detect_fulltime(row):
    texts = []
    for col in row.index:
        low = col.lower()
        if any(k in low for k in ("form", "форма", "mode", "study", "обуч")):
            texts.append(str(row[col]))
    return bool(RX_FULLTIME.search(" | ".join(texts)))

def is_paid(row):
    texts = []
    for col in row.index:
        low = col.lower()
        if any(k in low for k in ("basis", "основан", "fund", "финанс",
                                  "вид конкурса", "конкурс", "места", "basis_name")):
            texts.append(str(row[col]))
    return bool(RX_PAID.search(" | ".join(texts)))

def has_ege(row):
    texts = []
    for col in row.index:
        low = col.lower()
        if any(k in low for k in ("mark", "score", "exam", "subject",
                                  "оцен", "балл", "испытан", "предмет", "marks")):
            texts.append(str(row[col]))
    return bool(RX_EGE.search(" | ".join(texts)))

def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    """Магистратура + очная + без платных + без ЕГЭ + total_mark > 0."""
    m_master = df.apply(detect_master, axis=1)
    m_fulltime = df.apply(detect_fulltime, axis=1)
    m_paid = df.apply(is_paid, axis=1)
    m_ege = df.apply(has_ege, axis=1)

    if "total_mark" in df.columns:
        m_mark = pd.to_numeric(df["total_mark"], errors="coerce").fillna(0) > 0
    else:
        m_mark = True

    mask = m_master & m_fulltime & (~m_paid) & (~m_ege) & m_mark
    return df[mask].copy()

# ---------- main ----------

def main():
    print("Скачиваем все страницы: ?page=1..N&size=100")
    rows = iterate_all_pages()
    print(f"Всего записей собрано: {len(rows)}")

    if not rows:
        print("Данных не получили — проверьте доступность API.")
        sys.exit(1)

    df = json_normalize(rows, sep=".")
    # Переупорядочим полезные колонки (если есть)
    preferred = [c for c in (
        "id", "fio", "snils", "iin",
        "direction", "direction.name", "program", "program.name",
        "institute", "institute.name",
        "form", "study_form", "basis", "basis_name", "status",
        "priority", "total_mark", "score", "consent", "quota"
    ) if c in df.columns]
    other = [c for c in df.columns if c not in preferred]
    df = df[preferred + other]

    df_filtered = apply_filters(df)

    out_xlsx = "urfu_entrant_all.xlsx"
    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:
        df.to_excel(writer, sheet_name="all", index=False)
        df_filtered.to_excel(writer, sheet_name="masters_fulltime_budget", index=False)

        # авто-ширина
        for name, d in (("all", df), ("masters_fulltime_budget", df_filtered)):
            ws = writer.sheets[name]
            for i, col in enumerate(d.columns, 1):
                try:
                    max_len = max(len(str(col)), *(len(str(x)) for x in d[col].head(200)))
                except Exception:
                    max_len = len(str(col))
                ws.set_column(i-1, i-1, min(max(10, max_len + 2), 60))

    print(f"Готово: {out_xlsx}")
    print(f"Фильтр (магистратура, очная, без платных, без ЕГЭ, total_mark>0): {len(df_filtered)} строк")

if __name__ == "__main__":
    main()
