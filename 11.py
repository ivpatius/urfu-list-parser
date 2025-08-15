# -*- coding: utf-8 -*-
"""
УРФУ магистратура — автосбор только очной формы.
Стратегия: слушаем все XHR/Fetch ответы и вытаскиваем самые "тяжёлые" HTML,
особенно /fileadmin/ratings/*.html. Затем парсим нужные колонки и распределяем.

Зависимости:
    pip install playwright beautifulsoup4 pandas lxml
    python -m playwright install chromium
"""

import asyncio
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict

import pandas as pd
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

BASE_URL = "https://urfu.ru/ru/ratings-masters/"
OUT_XLSX = "urfu_ochnaya.xlsx"
OUT_CSV = "urfu_ochnaya.csv"

HEADLESS = False  # для визуального дебага поставь False и добавь slow_mo=150 в launch()

# --- эвристики и парсер рейтингового HTML ---

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
    """
    Пары 'Название' -> 'Значение' из шапки (table.supp.table-header).
    Нужные: Институт (филиал), Направление (образовательная программа), План приема.
    """
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
    """Парсит только очные секции из html выгрузки УрФУ + метаданные из шапки."""
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
            row_text_joined = " ".join(td.get_text(" ", strip=True) for td in tds)

            def cell(i):
                return tds[i].get_text(" ", strip=True) if i is not None and i < len(tds) else ""
            rec = {
                "ID": cell(idx_id),
                "Согласие на зачисление": cell(idx_con),
                "Приоритет": cell(idx_pr),
                "Сумма конкурсных баллов": cell(idx_sum),
                "Институт (филиал)": meta["Институт (филиал)"],
                "Направление (образовательная программа)": meta["Направление (образовательная программа)"],
                "План приема": meta["План приема"],
                "Источник": src_name,
                # Контекст всей строки + мета (для эвристики бюджета)
                "ROW_TEXT": (row_text_joined + " " + " ".join(meta.values())).strip(),
            }
            if rec["ID"]:
                rows.append(rec)
    return rows

# --- хранение перехваченных HTML ---

RATING_URL_RE = re.compile(r"/fileadmin/ratings/\d+_\d+_\d+\.html$", re.I)

@dataclass
class HtmlCandidate:
    url: str
    text: str
    size: int
    is_rating_url: bool
    looks_like_table: bool

# --- основной сценарий ---

async def run():
    candidates: Dict[str, HtmlCandidate] = {}
    seq = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)  # для отладки можно: slow_mo=150
        ctx = await pw.chromium.launch(headless=HEADLESS)      # простой контекст

        if hasattr(ctx, "new_page"):
            page = await ctx.new_page()
        else:
            ctx2 = await browser.new_context()
            page = await ctx2.new_page()

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
                    cand = HtmlCandidate(url=url, text=text, size=len(text), is_rating_url=is_rating, looks_like_table=looks_tbl)
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

        try:
            await page.context.close()
        except Exception:
            pass
        try:
            await browser.close()
        except Exception:
            pass

    if not candidates:
        print("Не удалось перехватить HTML-выгрузки. Включи HEADLESS=False и проверь, грузятся ли разделы.")
        return

    sorted_candidates = sorted(
        candidates.values(),
        key=lambda c: (not c.is_rating_url, -c.size)
    )

    all_rows: List[Dict] = []
    for cand in sorted_candidates:
        rows = parse_ratings_html(cand.text, src_name=Path(cand.url).name or cand.url)
        if rows:
            all_rows.extend(rows)

    if not all_rows:
        print("HTML получили, но таблиц очной формы не нашли. Проверь вручную самые крупные ответы в Network.")
        return

    df = pd.DataFrame(all_rows).drop_duplicates()

    # Приведение типов + извлечение чисел из строк вроде "17 мест"
    for col in ["Приоритет", "Сумма конкурсных баллов", "План приема"]:
        if col in df.columns:
            s = (
                df[col].astype(str)
                      .str.replace(",", ".", regex=False)
                      .str.extract(r"([-+]?\d*\.?\d+)")[0]
            )
            df[col] = pd.to_numeric(s, errors="coerce")

    # ---- ФИЛЬТР: только с согласием на бюджет (более гибко) ----
    def is_budget_consent(consent: str, row_text: str) -> bool:
        c = str(consent or "").lower()
        r = str(row_text or "").lower()
        has_yes = "да" in c
        is_paid = ("плат" in c) or ("контр" in c) or ("плат" in r) or ("контр" in r)
        has_budget_anywhere = ("бюд" in c) or ("бюд" in r) or ("общ" in r) or ("квот" in r)
        if not has_yes:
            return False
        if is_paid:
            return False
        if has_budget_anywhere:
            return True
        return True  # по умолчанию трактуем «Да» как бюджет

    if "Согласие на зачисление" in df.columns:
        df["ROW_TEXT"] = df.get("ROW_TEXT", "")
        before = len(df)
        df = df[df.apply(lambda x: is_budget_consent(x["Согласие на зачисление"], x.get("ROW_TEXT", "")), axis=1)]
        print(f"[filter] Бюджетные согласия: {len(df)} из {before}")

    if df.empty:
        df.drop(columns=["ROW_TEXT"], errors="ignore", inplace=True)
        df.to_excel(OUT_XLSX, index=False)
        df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print("После фильтра бюджетных согласий записей нет. Файлы сохранены пустыми.")
        return

    # ---- Ёмкости направлений (квоты) ----
    cap = (
        df.groupby(["Институт (филиал)", "Направление (образовательная программа)"])["План приема"]
          .max().fillna(0).astype(float).astype(int)
    )
    seats = cap.to_dict()  # (институт, направление) -> оставшиеся места

    # ---- РАУНДОВОЕ РАСПРЕДЕЛЕНИЕ ПО ПРИОРИТЕТАМ (чтобы направления заполнялись максимумом) ----
    df["Балл"] = df["Сумма конкурсных баллов"].fillna(0).astype(float)

    assigned_ids = set()
    assigned_rows: List[Dict] = []

    # максимальный реальный приоритет в данных
    max_pr = int(df["Приоритет"].dropna().max()) if not df["Приоритет"].dropna().empty else 0

    for p in range(1, max_pr + 1):
        # заявки текущего приоритета у ещё не распределённых
        round_apps = df[(~df["ID"].isin(assigned_ids)) & (df["Приоритет"] == p)].copy()
        if round_apps.empty:
            continue

        # по баллам ↓, при равенстве — по ID ↑
        round_apps = round_apps.sort_values(
            by=["Балл", "ID"],
            ascending=[False, True],
            kind="mergesort"
        )

        for _, rec in round_apps.iterrows():
            key = (rec["Институт (филиал)"], rec["Направление (образовательная программа)"])
            if seats.get(key, 0) > 0 and rec["ID"] not in assigned_ids:
                seats[key] -= 1
                assigned_ids.add(rec["ID"])
                assigned_rows.append({
                    "ID": rec["ID"],
                    "Согласие на зачисление": rec.get("Согласие на зачисление", ""),
                    "Приоритет": rec.get("Приоритет", None),
                    "Сумма конкурсных баллов": rec.get("Сумма конкурсных баллов", None),
                    "Институт (филиал)": rec.get("Институт (филиал)", ""),
                    "Направление (образовательная программа)": rec.get("Направление (образовательная программа)", ""),
                    "План приема": cap.get(key, 0),
                    "Источник": rec.get("Источник", "")
                })
        # следующий раунд p+1 будет добивать оставшиеся места

    result = pd.DataFrame(assigned_rows)

    # ---- Финал: чистим служебные поля, сортируем и сохраняем ----
    for tmp in ("ROW_TEXT", "Балл"):
        if tmp in result.columns:
            result.drop(columns=[tmp], inplace=True, errors="ignore")

    if not result.empty:
        result = result.sort_values(
            by=["Институт (филиал)", "Направление (образовательная программа)", "Приоритет", "Сумма конкурсных баллов"],
            ascending=[True, True, True, False],
            kind="mergesort"
        )

    final_cols = [
        "ID",
        "Согласие на зачисление",
        "Приоритет",
        "Сумма конкурсных баллов",
        "Институт (филиал)",
        "Направление (образовательная программа)",
        "План приема",
        "Источник"
    ]
    result = result[[c for c in final_cols if c in result.columns]].drop_duplicates()

    # Диагностика: какие направления остались недозаполнёнными
    unfilled = {k: v for k, v in seats.items() if v > 0}
    if unfilled:
        print("[info] Недозаполненные направления (осталось мест):")
        for (inst, prog), left in sorted(unfilled.items()):
            print(f"  - {inst} / {prog}: {left}")

    # Выгрузка
    result.to_excel(OUT_XLSX, index=False)
    result.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Готово (раундовое распределение по приоритетам):\n- {OUT_XLSX} (строк: {len(result)})\n- {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(run())
