# -*- coding: utf-8 -*-
"""
УРФУ магистратура — сбор очной формы ИЛИ чтение из файла + распределение (Deferred Acceptance с вытеснениями).
ЛОГИКА ОБНОВЛЕНА: при переполнении программы кандидат с БОЛЬШИМ баллом вытесняет слабейшего,
при равенстве баллов — выигрывает МЕНЬШИЙ приоритет (1 лучше 2 и т.д.), затем ID (лексикографически).

Зависимости:
    pip install playwright beautifulsoup4 pandas lxml openpyxl
    python -m playwright install chromium

Примеры:
    # Собрать с сайта и распределить
    python urfu_alloc.py --mode scrape --xlsx urfu_ochnaya.xlsx --csv urfu_ochnaya.csv

    # Прочитать из файла и распределить
    python urfu_alloc.py --mode file --input input.xlsx --xlsx output.xlsx
"""

import asyncio
import re
import heapq
import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple, Any, Optional

import pandas as pd
from bs4 import BeautifulSoup

# --------- Константы по умолчанию ---------
BASE_URL = "https://urfu.ru/ru/ratings-masters/"
DEFAULT_OUT_XLSX = "urfu_ochnaya.xlsx"
DEFAULT_OUT_CSV  = "urfu_ochnaya.csv"
HEADLESS_DEFAULT = True

# ========= Парсинг HTML (режим scrape) =========
try:
    from playwright.async_api import async_playwright
except Exception:
    async_playwright = None  # Позволяет запускать в режиме file без playwright

def looks_ochnaya(text: str) -> bool:
    t = (text or "").lower().replace("\xa0", " ")
    return ("очн" in t) or ("full-time" in t) or ("daytime" in t)

def find_idx(headers, keys):
    for i, h in enumerate(headers):
        low = h.lower().strip()
        for k in keys:
            if k in low:
                return i
    return None

def extract_header_meta(header_table) -> Dict[str, str]:
    meta = {
        "Институт (филиал)": "",
        "Направление (образовательная программа)": "",
        "План приема": ""
    }
    for tr in header_table.find_all("tr"):
        th = tr.find("th")
        td = tr.find("td")
        if not th or not td:
            continue
        name = th.get_text(" ", strip=True).lower()
        val  = td.get_text(" ", strip=True)
        if "институт" in name and "филиал" in name:
            meta["Институт (филиал)"] = val
        elif "направление" in name:
            meta["Направление (образовательная программа)"] = val
        elif ("план приема" in name) or ("план приёма" in name):
            meta["План приема"] = val
    return meta

def parse_ratings_html(html: str, src_name: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    rows: List[Dict] = []
    for hdr in soup.select("table.supp.table-header"):
        if not looks_ochnaya(hdr.get_text(" ", strip=True)):
            continue
        meta = extract_header_meta(hdr)
        tbl = hdr.find_next("table", class_="supp")
        if not tbl:
            continue
        head = tbl.find("tr")
        if not head:
            continue

        cols = [th.get_text(" ", strip=True) for th in head.find_all(["th", "td"])]
        idx_id  = find_idx(cols, ["номер абитуриента", "id участника", "id"])
        idx_con = find_idx(cols, ["согласие на зачисление"])
        idx_pr  = find_idx(cols, ["приоритет"])
        idx_sum = find_idx(cols, ["сумма конкурсных баллов"])
        if None in (idx_id, idx_con, idx_pr, idx_sum):
            continue

        for tr in tbl.find_all("tr")[1:]:
            tds = tr.find_all("td")
            if not tds:
                continue

            def cell(i):
                return tds[i].get_text(" ", strip=True) if i is not None and i < len(tds) else ""
            row_text_joined = " ".join(td.get_text(" ", strip=True) for td in tds)
            rec = {
                "ID": cell(idx_id),
                "Согласие на зачисление": cell(idx_con),
                "Приоритет": cell(idx_pr),
                "Сумма конкурсных баллов": cell(idx_sum),
                "Институт (филиал)": meta["Институт (филиал)"],
                "Направление (образовательная программа)": meta["Направление (образовательная программа)"],
                "План приема": meta["План приема"],
                "Источник": src_name,
                "ROW_TEXT": (row_text_joined + " " + " ".join(meta.values())).strip(),
            }
            if rec["ID"]:
                rows.append(rec)
    return rows

RATING_URL_RE = re.compile(r"/fileadmin/ratings/\d+_\d+_\d+\.html$", re.I)

@dataclass
class HtmlCandidate:
    url: str
    text: str
    size: int
    is_rating_url: bool
    looks_like_table: bool

async def scrape_dataframe(headless: bool = True) -> pd.DataFrame:
    """Сбор всех HTML с очной формой + парсинг в DataFrame."""
    if async_playwright is None:
        raise RuntimeError("Playwright не установлен. Установите: pip install playwright && python -m playwright install chromium")

    candidates: Dict[str, HtmlCandidate] = {}
    seq = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)
        try:
            ctx = await pw.chromium.launch(headless=headless)  # совместимость с ранними версиями
            page = await ctx.new_page()
        except Exception:
            ctx = await browser.new_context()
            page = await ctx.new_page()

        async def on_response(resp):
            nonlocal seq
            try:
                ct = (resp.headers.get("content-type", "") or "").lower()
                if "html" not in ct and "text" not in ct and ct != "":
                    return
                text = await resp.text()
                url = resp.url or f"synthetic://{seq}"
                seq += 1
                is_rating = bool(RATING_URL_RE.search(url))
                looks_tbl = ("table.supp" in text) or ("Номер абитуриента" in text and "Сумма конкурсных баллов" in text)
                if is_rating or looks_tbl:
                    cand = HtmlCandidate(url=url, text=text, size=len(text),
                                         is_rating_url=is_rating, looks_like_table=looks_tbl)
                    if url not in candidates or cand.size > candidates[url].size:
                        candidates[url] = cand
            except Exception:
                pass

        page.on("response", on_response)

        await page.goto(BASE_URL, wait_until="domcontentloaded")
        await page.wait_for_timeout(800)

        ochnaya = page.locator(
            "xpath=(//*[self::a or self::button or @role='button']"
            "[contains(translate(normalize-space(string(.)),'ОЧН','очн'),'очн')])"
        )
        count = await ochnaya.count()
        if count == 0:
            await page.wait_for_timeout(1200)
            count = await ochnaya.count()

        for i in range(count):
            try:
                el = await ochnaya.nth(i).element_handle()
                if not el:
                    continue
                await page.evaluate("(el)=>el.click()", el)
            except Exception:
                continue

            try:
                await page.wait_for_selector("a:has-text('Вернуться к выбору института')", timeout=4000)
            except Exception:
                pass

            # пролистываем якоря, чтобы подтянуть все куски
            try:
                for a in await page.locator("a[href^='#']").all():
                    try:
                        await a.click(timeout=400)
                    except Exception:
                        pass
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                await page.wait_for_timeout(400)
                await page.evaluate("window.scrollTo(0, 0)")
            except Exception:
                pass

            await page.wait_for_timeout(700)

            if page.url != BASE_URL:
                try:
                    await page.goto(BASE_URL, wait_until="domcontentloaded")
                except Exception:
                    pass
                await page.wait_for_timeout(300)

        try: await page.context.close()
        except Exception: pass
        try: await browser.close()
        except Exception: pass

    if not candidates:
        raise RuntimeError("Не удалось перехватить HTML-выгрузки (очн.). Запустите с --headless 0 и проверьте загрузку разделов.")

    sorted_candidates = sorted(candidates.values(),
                               key=lambda c: (not c.is_rating_url, -c.size))

    all_rows: List[Dict] = []
    for cand in sorted_candidates:
        all_rows.extend(parse_ratings_html(cand.text, src_name=Path(cand.url).name or cand.url))

    if not all_rows:
        raise RuntimeError("HTML получили, но таблиц очной формы не нашли.")

    df = pd.DataFrame(all_rows).drop_duplicates()
    return df

# ========= Унификация DataFrame и распределение =========

NUMERIC_COLS = ["Приоритет", "Сумма конкурсных баллов", "План приема"]

def coerce_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Надёжно вытащить числа из строк (например, '17 мест')."""
    for col in NUMERIC_COLS:
        if col in df.columns:
            s = (
                df[col].astype(str)
                      .str.replace(",", ".", regex=False)
                      .str.extract(r"([-+]?\d*\.?\d+)")[0]
            )
            df[col] = pd.to_numeric(s, errors="coerce")
    return df

def is_budget_consent(consent: str, row_text: str) -> bool:
    """Фильтр по 'Да (бюджет)' гибко. Исключаем платные/контракт."""
    c = str(consent or "").lower()
    r = str(row_text or "").lower()
    if "да" not in c:
        return False
    if ("плат" in c) or ("контр" in c) or ("плат" in r) or ("контр" in r):
        return False
    return True

def normalize_input_df(df: pd.DataFrame) -> pd.DataFrame:
    """Приводим вход к единому виду, фильтруем по согласию на бюджет (если есть)."""
    need_cols = [
        "ID",
        "Согласие на зачисление",
        "Приоритет",
        "Сумма конкурсных баллов",
        "Институт (филиал)",
        "Направление (образовательная программа)",
        "План приема",
    ]
    # Если нет ID — создадим
    if "ID" not in df.columns:
        df.insert(0, "ID", range(1, len(df) + 1))
    # Заполним отсутствующие служебные
    for c in need_cols:
        if c not in df.columns:
            df[c] = None

    # Приводим числа
    df = coerce_numeric(df)

    # Фильтр по согласию (если колонка есть)
    if "Согласие на зачисление" in df.columns:
        if "ROW_TEXT" not in df.columns:
            df["ROW_TEXT"] = ""
        df = df[df.apply(lambda x: is_budget_consent(x["Согласие на зачисление"], x.get("ROW_TEXT", "")), axis=1)].copy()

    return df

def build_capacities(df: pd.DataFrame) -> Dict[Tuple[str, str], int]:
    """Квоты направлений: берём максимум 'План приема' для (институт, программа)."""
    cap_series = (
        df.groupby(["Институт (филиал)", "Направление (образовательная программа)"])["План приема"]
          .max().fillna(0).astype(float).astype(int)
    )
    return cap_series.to_dict()

def _priority_of(rec: Dict[str, Any]) -> int:
    """Безопасно извлечь приоритет; если нет — очень большой (хуже всех)."""
    try:
        return int(rec.get("Приоритет", 10**9) or 10**9)
    except Exception:
        return 10**9

def deferred_acceptance_with_ejection(
    df: pd.DataFrame
) -> Tuple[pd.DataFrame, Dict[Tuple[str,str], int], Dict[Tuple[str,str], int]]:
    """
    Алгоритм отложенного зачисления c вытеснениями:
    - Каждый абитуриент пробует по своим приоритетам (по возрастанию).
    - Программа «держит» лучших в пределах квоты.
    - Критерий лучшести в ОЧЕРЕДИ ПРОГРАММЫ (обновлено):
        1) БОЛЬШЕ балл лучше;
        2) при равенстве баллов — МЕНЬШИЙ приоритет лучше (1 лучше 2);
        3) затем ID (лексикографически; больший ID «сильнее» при равенстве первых двух).
    """
    # Подготовка
    df["Балл"] = df["Сумма конкурсных баллов"].fillna(0).astype(float)
    df["Приоритет_norm"] = df["Приоритет"].fillna(1e9)
    capacities = build_capacities(df)

    # Список предпочтений по каждому ID: сортируем заявки по Приоритет ↑, при равенстве — по Баллу ↓
    prefs: Dict[str, List[Dict[str, Any]]] = (
        df.sort_values(by=["ID", "Приоритет_norm", "Балл"], ascending=[True, True, False], kind="mergesort")
          .groupby("ID").apply(lambda g: g.to_dict("records")).to_dict()
    )

    # Лучший балл по ID (чтобы сравнивать однорангово между заявками одного абитуриента)
    id_score: Dict[str, float] = df.groupby("ID")["Балл"].max().to_dict()

    next_idx: Dict[str, int] = {aid: 0 for aid in prefs.keys()}  # индекс следующего приоритета, который попробует абитуриент
    assigned_idx: Dict[str, int] = {}  # индекс текущего выбранного приоритета (куда зачислен)

    # Очередь свободных: по баллу ↓, затем ID ↑ (чтобы сильные предлагались первыми)
    free_queue: List[str] = sorted(prefs.keys(), key=lambda k: (-id_score.get(k, 0.0), str(k)))

    # Для каждой программы — мин-куча слабейших: (score, priority, id)
    program_heaps: Dict[Tuple[str, str], List[Tuple[float, int, str]]] = {k: [] for k in capacities.keys()}
    assigned: Dict[str, Tuple[Tuple[str, str], Dict[str, Any]]] = {}

    def try_propose(aid: str, pref_index: int) -> Tuple[bool, str]:
        """Абитуриент aid делает предложение по prefs[aid][pref_index]."""
        rec = prefs[aid][pref_index]
        key = (rec["Институт (филиал)"], rec["Направление (образовательная программа)"])
        cap = capacities.get(key, 0)
        if cap <= 0:
            return (False, "")
        heap = program_heaps.setdefault(key, [])

        # Сила заявки
        score = float(id_score.get(aid, rec.get("Сумма конкурсных баллов", 0.0)) or 0.0)
        prio  = _priority_of(rec)
        entry = (score, prio, str(aid))  # мин-куча: «слабейший» сверху

        # Если есть свободное место — просто добавляем
        if len(heap) < cap:
            heapq.heappush(heap, entry)
            assigned[aid] = (key, rec)
            assigned_idx[aid] = pref_index
            return (True, "")

        # Нет места — сравниваем с самым слабым
        weakest = heap[0]
        # Новый сильнее слабейшего, если:
        # 1) балл больше, ИЛИ
        # 2) балл равен, но приоритет меньше, ИЛИ
        # 3) балл и приоритет равны, но ID больше (лексикографически)
        is_stronger = (
            (entry[0] > weakest[0]) or
            (entry[0] == weakest[0] and entry[1] < weakest[1]) or
            (entry[0] == weakest[0] and entry[1] == weakest[1] and entry[2] > weakest[2])
        )
        if is_stronger:
            evicted_score, evicted_prio, evicted_id = heapq.heapreplace(heap, entry)
            if evicted_id in assigned:
                del assigned[evicted_id]
            assigned[aid] = (key, rec)
            assigned_idx[aid] = pref_index
            return (True, evicted_id)
        return (False, "")

    # --- Основной цикл ---
    while free_queue:
        aid = free_queue.pop(0)
        pl = prefs.get(aid, [])
        i = next_idx.get(aid, 0)

        # пропускаем варианты без квоты (0 или NaN)
        while i < len(pl) and capacities.get((pl[i]["Институт (филиал)"], pl[i]["Направление (образовательная программа)"]), 0) <= 0:
            i += 1
        if i >= len(pl):
            continue  # вариантов не осталось

        accepted, evicted = try_propose(aid, i)
        if accepted:
            if evicted:
                # вытеснённый продолжает с СЛЕДУЮЩЕГО приоритета
                prev = assigned_idx.get(evicted, -1)
                next_idx[evicted] = max(prev + 1, next_idx.get(evicted, 0))
                if next_idx[evicted] < len(prefs.get(evicted, [])):
                    free_queue.insert(0, evicted)  # приоритетная повторная попытка
        else:
            # отказ — пробуем следующий приоритет
            next_idx[aid] = i + 1
            if next_idx[aid] < len(pl):
                free_queue.append(aid)

    # --- Результат ---
    rows_out: List[Dict] = []
    for aid, (key, rec) in assigned.items():
        inst, prog = key
        rows_out.append({
            "ID": rec.get("ID", aid),
            "Согласие на зачисление": rec.get("Согласие на зачисление", ""),
            "Приоритет": rec.get("Приоритет", None),
            "Сумма конкурсных баллов": rec.get("Сумма конкурсных баллов", None),
            "Институт (филиал)": inst,
            "Направление (образовательная программа)": prog,
            "План приема": int(build_capacities(df).get(key, 0)),
            "Источник": rec.get("Источник", ""),
            "Зачислен на": prog
        })

    result = pd.DataFrame(rows_out).drop_duplicates()

    # Сортировка результата для стабильного просмотра
    if not result.empty:
        result = result.sort_values(
            by=[
                "Институт (филиал)",
                "Направление (образовательная программа)",
                "Сумма конкурсных баллов",   # по баллу ↓
                "Приоритет",                  # при равенстве — по приоритету ↑
                "ID"                          # затем по ID ↑
            ],
            ascending=[True, True, False, True, True],
            kind="mergesort"
        )

    # Диагностика: сколько мест осталось (по содержимому куч)
    remain: Dict[Tuple[str, str], int] = {}
    # Восстановим текущие размеры куч
    # (program_heaps содержит финальные наборы; их длины = занятым местам)
    for key, cap in build_capacities(df).items():
        left = cap - len(program_heaps.get(key, []))
        if left > 0:
            remain[key] = left

    return result, build_capacities(df), remain

# ========= IO и CLI =========

def read_any_table(path: str) -> pd.DataFrame:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Файл не найден: {path}")
    if p.suffix.lower() in [".xlsx", ".xls"]:
        return pd.read_excel(p)
    elif p.suffix.lower() in [".csv"]:
        return pd.read_csv(p)
    else:
        # пробуем как Excel
        try:
            return pd.read_excel(p)
        except Exception:
            # пробуем как CSV в UTF-8
            return pd.read_csv(p, encoding="utf-8")

def save_outputs(df: pd.DataFrame, xlsx_path: Optional[str], csv_path: Optional[str]):
    if xlsx_path:
        df.to_excel(xlsx_path, index=False)
    if csv_path:
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")

def main():
    ap = argparse.ArgumentParser(description="УРФУ: сбор/чтение + распределение (Deferred Acceptance, обновлённая логика вытеснения).")
    ap.add_argument("--mode", choices=["scrape", "file"], default="scrape", help="Источник данных: scrape (сайт) или file (Excel/CSV).")
    ap.add_argument("--input", type=str, help="Путь к входному файлу (для --mode file).")
    ap.add_argument("--xlsx", type=str, default=DEFAULT_OUT_XLSX, help="Путь для сохранения результата в Excel.")
    ap.add_argument("--csv",  type=str, default=DEFAULT_OUT_CSV,  help="Путь для сохранения результата в CSV.")
    ap.add_argument("--headless", type=int, default=1, help="1 — без интерфейса браузера, 0 — с интерфейсом (для отладки).")
    args = ap.parse_args()

    # 1) Получение исходного df
    if args.mode == "scrape":
        df_raw = asyncio.run(scrape_dataframe(headless=bool(args.headless)))
    else:
        if not args.input:
            raise SystemExit("Укажите --input путь к файлу для --mode file")
        df_raw = read_any_table(args.input)

    # 2) Нормализация и фильтр
    df = normalize_input_df(df_raw)

    # Если после фильтра пусто — просто сохраняем пустые файлы
    if df.empty:
        save_outputs(df.drop(columns=["ROW_TEXT"], errors="ignore"), args.xlsx, args.csv)
        print("После фильтра записей нет. Файлы сохранены пустыми.")
        return

    # 3) Распределение
    result, capacities, remain = deferred_acceptance_with_ejection(df)

    # 4) Сохранение
    save_outputs(result, args.xlsx, args.csv)

    # 5) Диагностика
    if remain:
        print("[info] Недозаполненные направления (осталось мест):")
        for (inst, prog), left in sorted(remain.items()):
            print(f"  - {inst} / {prog}: {left}")

    print(f"Готово (устойчивое распределение с обновлёнными правилами вытеснения):\n- {args.xlsx} (строк: {len(result)})\n- {args.csv}")

if __name__ == "__main__":
    main()
