# update_numbers_db_from_rakuten_v1.py
# ============================================================
# Numbers AI v7 DB更新専用スクリプト
#
# 目的:
#   楽天×宝くじのバックナンバーから最新の当せん番号を取得し、
#   numbers.db 内の auto_draw_results テーブルへ保存する。
#
# 重要:
#   numbers3_enriched / numbers4_enriched は壊さない。
#   取得結果は auto_draw_results に保存し、アプリ側で既存DBと統合して使う。
#
# 実行例:
#   py -3.14-64 update_numbers_db_from_rakuten_v1.py --db-path numbers.db
# ============================================================

import argparse
import re
import sqlite3
import urllib.request
from datetime import datetime
from html import unescape
from pathlib import Path

import pandas as pd


AUTO_DRAW_TABLE = "auto_draw_results"

RAKUTEN_URLS = {
    "N3": "https://takarakuji.rakuten.co.jp/backnumber/numbers3/",
    "N4": "https://takarakuji.rakuten.co.jp/backnumber/numbers4/",
}


def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_auto_draw_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {AUTO_DRAW_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game TEXT NOT NULL,
        draw_round INTEGER NOT NULL,
        draw_date TEXT,
        number TEXT NOT NULL,
        source TEXT,
        fetched_at TEXT NOT NULL,
        UNIQUE(game, draw_round)
    )
    """)
    conn.commit()


def normalize_number(value, digits: int) -> str | None:
    if value is None:
        return None

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return None

    s = re.sub(r"\D", "", s)
    if not s:
        return None

    return s.zfill(digits)[-digits:]


def normalize_date(value) -> str:
    if value is None:
        return ""

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""

    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    return s


def fetch_url_text(url: str) -> str:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0 Safari/537.36"
            ),
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        },
    )

    with urllib.request.urlopen(req, timeout=30) as res:
        raw = res.read()

    return raw.decode("utf-8", errors="replace")


def html_to_text(html: str) -> str:
    html = re.sub(r"(?is)<script.*?</script>", " ", html)
    html = re.sub(r"(?is)<style.*?</style>", " ", html)
    html = re.sub(r"(?i)<br\s*/?>", "\n", html)
    html = re.sub(r"(?i)</(tr|p|div|li|h1|h2|h3|dt|dd|th|td)>", "\n", html)
    text = re.sub(r"(?s)<.*?>", " ", html)
    text = unescape(text)
    text = re.sub(r"[ \t\r\f\v]+", " ", text)
    text = re.sub(r"\n\s+", "\n", text)
    return text


def parse_rakuten_latest(game: str, digits: int) -> list[dict]:
    """
    楽天×宝くじの当せん番号ページから当せん結果を取得する。
    HTML直読み + pandas.read_html の二段構え。
    """
    url = RAKUTEN_URLS[game]
    rows = []

    # 1) HTML直読み
    try:
        html = fetch_url_text(url)
        text = html_to_text(html)

        pattern = re.compile(
            r"回号\s*第\s*(\d+)\s*回\s*"
            r".{0,160}?"
            r"抽せん日\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2})\s*"
            r".{0,160}?"
            r"当せん番号\s*([0-9]{" + str(digits) + r"})",
            re.DOTALL,
        )

        for m in pattern.finditer(text):
            rows.append({
                "round": int(m.group(1)),
                "date": normalize_date(m.group(2)),
                "number": normalize_number(m.group(3), digits),
            })

        # ブロック単位の保険
        if not rows:
            blocks = re.split(r"(?=回号\s*第\s*\d+\s*回)", text)

            for block in blocks:
                round_match = re.search(r"回号\s*第\s*(\d+)\s*回", block)
                date_match = re.search(r"抽せん日\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2})", block)
                number_match = re.search(r"当せん番号\s*([0-9]{" + str(digits) + r"})", block)

                if round_match and number_match:
                    rows.append({
                        "round": int(round_match.group(1)),
                        "date": normalize_date(date_match.group(1)) if date_match else "",
                        "number": normalize_number(number_match.group(1), digits),
                    })

    except Exception as e:
        print(f"[WARN] HTML direct fetch failed for {game}: {e}")

    # 2) pandas.read_html の保険
    try:
        tables = pd.read_html(url)
    except Exception:
        tables = []

    for df in tables:
        if df.empty:
            continue

        text = "\n".join(df.astype(str).fillna("").values.flatten().tolist())

        pattern = re.compile(
            r"第\s*(\d+)\s*回.*?"
            r"(\d{4}[/-]\d{1,2}[/-]\d{1,2}).*?"
            r"([0-9]{" + str(digits) + r"})",
            re.DOTALL,
        )

        for m in pattern.finditer(text):
            rows.append({
                "round": int(m.group(1)),
                "date": normalize_date(m.group(2)),
                "number": normalize_number(m.group(3), digits),
            })

    # 重複排除
    uniq = {}
    for r in rows:
        if r.get("round") and r.get("number"):
            uniq[int(r["round"])] = r

    return [uniq[k] for k in sorted(uniq.keys())]


def save_auto_draws(conn: sqlite3.Connection, game: str, rows: list[dict]) -> int:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    count = 0

    for r in rows:
        if not r.get("round") or not r.get("number"):
            continue

        conn.execute(f"""
        INSERT INTO {AUTO_DRAW_TABLE}
        (game, draw_round, draw_date, number, source, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(game, draw_round) DO UPDATE SET
            draw_date = excluded.draw_date,
            number = excluded.number,
            source = excluded.source,
            fetched_at = excluded.fetched_at
        """, (
            game,
            int(r["round"]),
            r.get("date", ""),
            r["number"],
            "rakuten",
            now,
        ))
        count += 1

    conn.commit()
    return count


def latest_saved(conn: sqlite3.Connection, game: str) -> dict | None:
    row = conn.execute(
        f"""
        SELECT game, draw_round, draw_date, number, source, fetched_at
        FROM {AUTO_DRAW_TABLE}
        WHERE game = ?
        ORDER BY draw_round DESC
        LIMIT 1
        """,
        (game,),
    ).fetchone()

    if row is None:
        return None

    return dict(row)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="numbers.db")
    parser.add_argument("--latest-only", action="store_true", help="最新1件だけ保存する")
    args = parser.parse_args()

    db_path = Path(args.db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"numbers.db が見つかりません: {db_path.resolve()}")

    print("=== update_numbers_db_from_rakuten_v1 ===")
    print(f"db_path = {db_path}")

    conn = connect_db(str(db_path))
    ensure_auto_draw_table(conn)

    for game, digits in [("N3", 3), ("N4", 4)]:
        print(f"\n=== fetch {game} ===")
        rows = parse_rakuten_latest(game, digits)

        if args.latest_only and rows:
            rows = [max(rows, key=lambda x: int(x["round"]))]

        print(f"fetched rows = {len(rows)}")

        if rows:
            latest = max(rows, key=lambda x: int(x["round"]))
            print(f"latest fetched = 第{latest['round']}回 {latest.get('date', '')} {latest['number']}")

        saved = save_auto_draws(conn, game, rows)
        print(f"saved rows = {saved}")

        latest_db = latest_saved(conn, game)
        if latest_db:
            print(
                "latest db = "
                f"第{latest_db['draw_round']}回 "
                f"{latest_db['draw_date']} "
                f"{latest_db['number']} "
                f"source={latest_db['source']}"
            )

    print("\n=== done ===")


if __name__ == "__main__":
    main()
