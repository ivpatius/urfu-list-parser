
# -*- coding: utf-8 -*-
"""
HTML→Excel (много файлов, автоопределение кодировки).

Новое в этой версии:
- Автоопределение кодировки (ищем <meta charset=...>, XML-decl; пытаемся UTF-8 → CP1251).
- Можно вручную задать кодировку через --encoding (auto|utf-8|cp1251|latin-1).
- Быстрый ручной парсер 'soup' по умолчанию (устойчив к «грязной» вёрстке).
- Без applymap: очистка строк делается столбцово через .map (без FutureWarning).

Примеры:
  python parse_html_to_excel_enc.py --folder "path/to/htmls" --out result.xlsx
  python parse_html_to_excel_enc.py --folder . --engine soup                 # по умолчанию
  python parse_html_to_excel_enc.py --folder . --encoding cp1251             # если уверены в 1251
"""
import argparse
import sys
import re
from pathlib import Path
from typing import List, Tuple, Optional
import io
import pandas as pd

# ---------- helpers ----------
def safe_sheet_name(name: str) -> str:
    name = re.sub(r'[:\\/*?\[\]]', '_', name)
    return name[:31] if len(name) > 31 else name

def sniff_encoding(path: Path) -> str:
    """Very lightweight encoding detector: meta tags → try utf-8 → cp1251 → latin-1."""
    raw = path.read_bytes()
    head = raw[:200_000]  # read only head for speed
    # 1) meta charset / XML declaration
    m = re.search(br'charset\s*=\s*(["\'])?\s*([A-Za-z0-9_\-]+)\s*\1', head, flags=re.IGNORECASE)
    if m:
        enc = m.group(2).decode('ascii', 'ignore').lower()
        return enc
    m = re.search(br'encoding\s*=\s*["\']\s*([A-Za-z0-9_\-]+)\s*["\']', head, flags=re.IGNORECASE)
    if m:
        enc = m.group(1).decode('ascii', 'ignore').lower()
        return enc
    # 2) try utf-8 strict
    try:
        head.decode('utf-8')
        return 'utf-8'
    except UnicodeDecodeError:
        pass
    # 3) try cp1251
    try:
        head.decode('cp1251')
        return 'cp1251'
    except UnicodeDecodeError:
        pass
    # 4) fallback
    return 'latin-1'

def read_text_with_encoding(path: Path, forced: Optional[str]) -> Tuple[str, str]:
    if forced and forced.lower() != 'auto':
        enc = forced.lower()
    else:
        enc = sniff_encoding(path)
    # final read
    text = path.read_text(encoding=enc, errors='strict' if enc in ('utf-8','cp1251') else 'ignore')
    return text, enc

# ---------- Parsers ----------
def read_tables_with_pandas_text(html_text: str, engine: str) -> List[pd.DataFrame]:
    if engine not in {"lxml", "bs4", "html5lib"}:
        return []
    flavor = [engine]
    try:
        tables = pd.read_html(io.StringIO(html_text), flavor=flavor, header=0)
        if not tables:
            tables = pd.read_html(io.StringIO(html_text), flavor=flavor, header=None)
        return tables
    except ImportError:
        return []
    except Exception:
        return []

def read_tables_with_bs4_manual(html_text: str) -> List[pd.DataFrame]:
    try:
        from bs4 import BeautifulSoup
    except Exception:
        return []
    # prefer lxml if installed, else builtin
    parser = 'lxml'
    try:
        import lxml  # noqa: F401
    except Exception:
        parser = 'html.parser'
    soup = BeautifulSoup(html_text, parser)
    dfs: List[pd.DataFrame] = []
    for tbl in soup.find_all('table'):
        rows = []
        for tr in tbl.find_all('tr'):
            cells = tr.find_all(['th','td']) or []
            if not cells:
                continue
            rows.append([c.get_text(strip=True) for c in cells])
        if not rows:
            continue
        max_len = max(len(r) for r in rows)
        rows = [r + [''] * (max_len - len(r)) for r in rows]
        df = pd.DataFrame(rows)
        # try promote header
        if df.shape[0] > 1:
            first = df.iloc[0].astype(str).tolist()
            uniq_ratio = len(set(first)) / max(1, len(first))
            has_letters = sum(bool(re.search(r"[A-Za-zА-Яа-я]", s)) for s in first) >= max(1, len(first)//2)
            if uniq_ratio > 0.8 and has_letters:
                df.columns = first
                df = df.iloc[1:].reset_index(drop=True)
        dfs.append(df)
    return dfs

def extract_all_tables(path: Path, engine_order: List[str], forced_encoding: Optional[str]) -> List[pd.DataFrame]:
    html_text, enc = read_text_with_encoding(path, forced_encoding)
    tables: List[pd.DataFrame] = []
    for eng in engine_order:
        if eng == 'soup':
            tables = read_tables_with_bs4_manual(html_text)
        else:
            tables = read_tables_with_pandas_text(html_text, eng)
        if tables:
            break
    # Clean strings per column
    cleaned: List[pd.DataFrame] = []
    for i, df in enumerate(tables, start=1):
        df = df.copy()
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
        df.insert(0, '__source_file', path.name)
        df.insert(1, '__table_no', i)
        df['__row_in_table'] = range(1, len(df) + 1)
        cleaned.append(df)
    return cleaned

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description='Parse HTML tables from a folder into a single Excel workbook (with encoding autodetect).')
    ap.add_argument('--folder', type=str, default='.', help='Folder with *.html/*.htm files (default: .)')
    ap.add_argument('--out', type=str, default='parsed_html_tables.xlsx', help='Output Excel file path.')
    ap.add_argument('--pattern', type=str, default='*.htm,*.html', help='Comma-separated globs.')
    ap.add_argument('--engine', type=str, default='soup', choices=['auto','lxml','bs4','html5lib','soup'],
                    help='Engine: auto tries lxml→bs4→html5lib→soup. Default: soup')
    ap.add_argument('--encoding', type=str, default='auto', help='Force encoding: auto|utf-8|cp1251|latin-1')
    args = ap.parse_args()

    root = Path(args.folder).expanduser().resolve()
    patterns = [p.strip() for p in args.pattern.split(',') if p.strip()]
    files: List[Path] = []
    for pat in patterns:
        files.extend(sorted(root.glob(pat)))
    if not files:
        print(f'No HTML files found in: {root} (patterns: {patterns})', file=sys.stderr)
        sys.exit(2)

    engine_order = ["lxml","bs4","html5lib","soup"] if args.engine == 'auto' else [args.engine]
    print(f'Found {len(files)} files. Engine order: {engine_order}. Encoding: {args.encoding}')

    all_rows: List[pd.DataFrame] = []
    per_file_first_table: List[Tuple[str, pd.DataFrame]] = []

    for idx, f in enumerate(files, start=1):
        print(f'[{idx}/{len(files)}] {f.name} ...', end='', flush=True)
        tables = extract_all_tables(f, engine_order, args.encoding)
        if not tables:
            print(' no tables.')
            stub = pd.DataFrame({'__source_file':[f.name], '__table_no':[None], '__row_in_table':[None]})
            per_file_first_table.append((f.name, stub))
            all_rows.append(stub)
            continue
        per_file_first_table.append((f.name, tables[0]))
        all_rows.extend(tables)
        print(f' {len(tables)} table(s).')

    big = pd.concat(all_rows, ignore_index=True)
    out_path = Path(args.out).expanduser().resolve()
    with pd.ExcelWriter(out_path, engine='xlsxwriter') as xw:
        big.to_excel(xw, index=False, sheet_name='all_tables')
        for fname, df in per_file_first_table:
            sheet = safe_sheet_name(Path(fname).stem)
            try:
                df.to_excel(xw, index=False, sheet_name=sheet)
            except Exception:
                sheet = safe_sheet_name(sheet[:27] + '_aux')
                df.to_excel(xw, index=False, sheet_name=sheet)
    print(f'Saved: {out_path}')

if __name__ == '__main__':
    main()
