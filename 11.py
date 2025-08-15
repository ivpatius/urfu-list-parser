# -*- coding: utf-8 -*-
"""
УРФУ магистратура — автосбор только очной формы.
Собираем HTML-рейтинги, парсим, фильтруем по "Да, бюджет" (гибко) и
распределяем абитуриентов алгоритмом отложенного принятия (Gale–Shapley style):
кандидаты идут по приоритетам 1→2→…, программы держат у себя топ по баллам
до заполнения квоты и вытесняют слабейших при приходе более сильных.
Итог сохраняется в urfu_ochnaya.xlsx / urfu_ochnaya.csv.

Зависимости:
    pip install playwright beautifulsoup4 pandas lxml
    python -m playwright install chromium
"""

import asyncio
import re
import heapq
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Tuple, Any

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
    Достаём пары 'Название' -> 'Значение' из шапки (table.supp.table-header).
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
    """Парсит только очные секции из html, добавляя метаданные из шапки."""
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
                "ROW_TEXT": (row_text_joined + " " + " ".join(meta.values())).strip(),  # для гибкого фильтра бюджета
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

    # ---- ФИЛЬТР: только с согласием на бюджет (гибко) ----
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

    # ---- Квоты (ёмкости) направлений ----
    cap_series = (
        df.groupby(["Институт (филиал)", "Направление (образовательная программа)"])["План приема"]
          .max().fillna(0).astype(float).astype(int)
    )
    seats: Dict[Tuple[str, str], int] = cap_series.to_dict()

    # ---- Подготовка данных для алгоритма отложенного принятия ----
    # Нормализуем балл и приоритет
    df["Балл"] = df["Сумма конкурсных баллов"].fillna(0).astype(float)
    df["Приоритет_norm"] = df["Приоритет"].fillna(1e9)

    # Список предпочтений по каждому абитуриенту: по приоритетам возр.
    # Каждый элемент — исходная строка (dict), чтобы отдать в результат выбранный вариант.
    prefs: Dict[str, List[Dict[str, Any]]] = (
        df.sort_values(by=["ID", "Приоритет_norm", "Балл"], ascending=[True, True, False], kind="mergesort")
          .groupby("ID")
          .apply(lambda g: g.to_dict("records"))
          .to_dict()
    )

    # Балл на ID (обычно одинаков во всех заявках)
    id_score = df.groupby("ID")["Балл"].max().to_dict()

    # Очередь свободных абитуриентов и индекс следующего приоритета (куда ещё не предлагали)
    next_choice_idx: Dict[str, int] = {aid: 0 for aid in prefs.keys()}
    # Начальная очередь — все ID, сортировка по баллам ↓, затем ID ↑ для детерминизма
    free_queue: List[str] = sorted(prefs.keys(), key=lambda k: (-id_score.get(k, 0.0), str(k)))

    # Для каждой программы держим min-heap по (балл, ID) — на вершине слабейший
    program_heaps: Dict[Tuple[str, str], List[Tuple[float, str]]] = {k: [] for k in seats.keys()}
    # Текущее назначение: ID -> (key, chosen_rec)
    assigned: Dict[str, Tuple[Tuple[str, str], Dict[str, Any]]] = {}

    def try_propose(aid: str, rec: Dict[str, Any]) -> Tuple[bool, str]:
        """Кандидат aid делает предложение программе rec. Возвращает (accepted, evicted_id or '')."""
        key = (rec["Институт (филиал)"], rec["Направление (образовательная программа)"])
        cap = seats.get(key, 0)
        if cap <= 0:
            return (False, "")
        heap = program_heaps.setdefault(key, [])
        score = float(id_score.get(aid, rec.get("Сумма конкурсных баллов", 0.0)) or 0.0)
        item = (score, str(aid))
        if len(heap) < cap:
            heapq.heappush(heap, item)
            assigned[aid] = (key, rec)
            return (True, "")
        # сравним с текущим слабейшим
        weakest = heap[0]
        if (score, str(aid)) > weakest:  # лучше — вытесняем
            evicted_score, evicted_id = heapq.heapreplace(heap, item)
            # найдём и уберём предыдущее назначение вытеснённого
            if evicted_id in assigned:
                del assigned[evicted_id]
            assigned[aid] = (key, rec)
            return (True, evicted_id)
        return (False, "")

    # --- Основной цикл отложенного принятия ---
    while free_queue:
        aid = free_queue.pop(0)
        prefs_list = prefs.get(aid, [])
        i = next_choice_idx.get(aid, 0)
        # пропускаем пустые/нулевые приоритеты
        while i < len(prefs_list) and seats.get((prefs_list[i]["Институт (филиал)"],
                                                prefs_list[i]["Направление (образовательная программа)"]), 0) == 0:
            i += 1
        if i >= len(prefs_list):
            continue  # у кандидата не осталось вариантов
        rec = prefs_list[i]
        accepted, evicted_id = try_propose(aid, rec)
        if accepted:
            next_choice_idx[aid] = i  # зафиксировали, куда встал (для протокола)
            if evicted_id:
                # вытеснённый идёт предлагаться дальше (следующий приоритет после того, где его вытеснили)
                next_choice_idx[evicted_id] = (next_choice_idx.get(evicted_id, 0) + 1)
                # если у него остались варианты — вернём в очередь, причём повыше (чтобы быстрее перераспределился)
                if next_choice_idx[evicted_id] < len(prefs.get(evicted_id, [])):
                    free_queue.insert(0, evicted_id)
        else:
            # отказ — пробуем следующий приоритет
            next_choice_idx[aid] = i + 1
            if next_choice_idx[aid] < len(prefs_list):
                free_queue.append(aid)

    # --- Формируем результат из назначений ---
    assigned_rows: List[Dict] = []
    for aid, (key, rec) in assigned.items():
        inst, prog = key
        assigned_rows.append({
            "ID": rec.get("ID", aid),
            "Согласие на зачисление": rec.get("Согласие на зачисление", ""),
            "Приоритет": rec.get("Приоритет", None),
            "Сумма конкурсных баллов": rec.get("Сумма конкурсных баллов", None),
            "Институт (филиал)": inst,
            "Направление (образовательная программа)": prog,
            "План приема": int(cap_series.get(key, 0)),
            "Источник": rec.get("Источник", "")
        })

    result = pd.DataFrame(assigned_rows)

    # Чистим служебные поля в исходном df (если будем сохранять промежуточный)
    df.drop(columns=["ROW_TEXT", "Балл", "Приоритет_norm"], errors="ignore", inplace=True)

    # Финальная сортировка и порядок колонок
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

    # Диагностика: оставшиеся свободные места по программам
    # (факт может отличаться от cap - len(heap), если ни у кого нет заявки на программу)
    remain = {}
    for key, cap in seats.items():
        heap = program_heaps.get(key, [])
        left = max(int(cap) - len(heap), 0)
        if left > 0:
            remain[key] = left
    if remain:
        print("[info] Недозаполненные направления (осталось мест):")
        for (inst, prog), left in sorted(remain.items()):
            print(f"  - {inst} / {prog}: {left}")

    # Выгрузка
    result.to_excel(OUT_XLSX, index=False)
    result.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
    print(f"Готово (отложенное принятие с вытеснениями):\n- {OUT_XLSX} (строк: {len(result)})\n- {OUT_CSV}")

if __name__ == "__main__":
    asyncio.run(run())
