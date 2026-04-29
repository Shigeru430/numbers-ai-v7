# app_numbers_v7_pro_db_history_rakuten_v13_simulation_stats.py
# ============================================================
# Numbers AI v7
# UI: app_numbers_v7_pro_fix_full.py ベース
# DB連携 + 楽天最新結果確認 + 完全自動履歴 + メール送信版 Rakuten v19（公開版・メール機能なし）
#
# 修正点:
# 1. 予想回が同じなら予想結果をDB保存済みのものから再表示
# 2. 日付が変わっても同じ抽選回の予想はブレない
# 3. HTMLがコード表示される問題を修正
# 4. ロジック文字を読みやすい黒文字に修正
# 5. カードの黒バー化を避けるため、カードは1つのHTMLで描画
# ============================================================

import json
import re
import sqlite3
import textwrap
import urllib.request
from html import unescape
from datetime import datetime
from pathlib import Path
from collections import Counter, defaultdict

import pandas as pd
import streamlit as st


# =====================================================
# 基本設定
# =====================================================
APP_TITLE = "Numbers AI v7"
DEFAULT_DB_PATH = "numbers.db"

TABLE_N3 = "numbers3_enriched"
TABLE_N4 = "numbers4_enriched"

HISTORY_TABLE = "prediction_history_v7"
AUTO_DRAW_TABLE = "auto_draw_results"

RANK_N3 = [4, 5, 9, 11, 20]
RANK_N4 = [3, 6, 7, 9, 17]

HISTORY_LIMIT = 30

SIM_N3_CSV = "sim_numbers3.csv"
SIM_N4_CSV = "sim_numbers4.csv"

RAKUTEN_URLS = {
    "N3": "https://takarakuji.rakuten.co.jp/backnumber/numbers3/",
    "N4": "https://takarakuji.rakuten.co.jp/backnumber/numbers4/",
}


st.set_page_config(
    page_title=APP_TITLE,
    page_icon="🎯",
    layout="wide"
)


# =====================================================
# CSS
# =====================================================
st.markdown("""
<style>
html, body, [class*="css"] {
    background-color: #ffffff;
    color: #242938;
    font-family: "Segoe UI", "Roboto", sans-serif;
}

.stApp {
    background: #ffffff;
}

.block-container {
    padding-top: 3rem;
    padding-bottom: 2rem;
    max-width: 1200px;
}

#MainMenu {visibility: hidden;}
footer {visibility: hidden;}

.stTabs [data-baseweb="tab-list"] {
    gap: 22px;
    background: transparent !important;
    border-bottom: 1px solid rgba(36,41,56,.12);
}

.stTabs [data-baseweb="tab"] {
    font-size: 16px;
    font-weight: 700;
    color: #242938;
}

.stTabs [aria-selected="true"] {
    color: #ff4d6d !important;
}

.app-title {
    font-size: 38px;
    font-weight: 900;
    color: #6C5CE7;
    text-shadow: 0 0 14px rgba(108,92,231,.45);
    letter-spacing: .5px;
}

.app-sub {
    color: #7c8496;
    font-size: 15px;
    margin-top: 6px;
    margin-bottom: 18px;
}

.status-row {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
    margin: 22px 0 24px;
}

.status-box {
    background: linear-gradient(135deg, #171b28, #11141e);
    border-radius: 18px;
    padding: 16px 18px;
    border: 1px solid rgba(255,255,255,.08);
    box-shadow: 0 12px 28px rgba(0,0,0,.25);
}

.status-label {
    color: #9ca3af;
    font-size: 13px;
}

.status-value {
    color: #f4f6ff;
    font-size: 22px;
    font-weight: 900;
    margin-top: 4px;
}

.pred-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 18px;
}

.pred-card {
    background: linear-gradient(135deg, #171b28, #11141e);
    border-radius: 22px;
    padding: 28px;
    box-shadow: 0 16px 36px rgba(0,0,0,.35);
    border: 1px solid rgba(255,255,255,.08);
    margin-bottom: 20px;
}

.main-card-title {
    color: #7b6cff;
    font-size: 24px;
    font-weight: 900;
    margin-bottom: 18px;
}

.pred-number {
    font-size: 56px;
    font-weight: 950;
    font-family: "Consolas", "Courier New", monospace;
    text-align: center;
    letter-spacing: 6px;
    margin: 14px 0;
}

.strong {
    color: #00ffc6;
    text-shadow: 0 0 14px rgba(0,255,198,.35);
}

.mid {
    color: #ffd166;
    text-shadow: 0 0 12px rgba(255,209,102,.28);
}

.weak {
    color: #f4f6ff;
}

.rank-text {
    text-align: center;
    color: #9ca3af;
    font-size: 14px;
    margin-top: 18px;
}

.logic-card {
    background: #ffffff;
    border: 1px solid rgba(36,41,56,.12);
    border-radius: 22px;
    padding: 28px;
    box-shadow: 0 14px 34px rgba(15,17,23,.08);
    margin-top: 24px;
}

.logic-title {
    font-size: 32px;
    font-weight: 900;
    color: #242938;
    margin-bottom: 20px;
}

.logic-text {
    color: #242938;
    font-size: 17px;
    line-height: 1.9;
    font-weight: 650;
}

.history-title {
    font-size: 34px;
    font-weight: 900;
    color: #242938;
    margin: 26px 0 20px;
}

.history-card {
    background: #151a26;
    padding: 24px 28px;
    border-radius: 18px;
    margin-bottom: 18px;
    box-shadow: 0 12px 30px rgba(0,0,0,.28);
    border: 1px solid rgba(255,255,255,.08);
}

.history-head {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 20px;
    color: #d7dbea;
    font-size: 16px;
    font-weight: 800;
}

.history-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 32px;
}

.history-label {
    color: #7b6cff;
    font-size: 18px;
    font-weight: 900;
    margin-bottom: 8px;
}

.history-pred {
    color: #f4f6ff;
    font-size: 25px;
    font-weight: 900;
    font-family: "Consolas", "Courier New", monospace;
    letter-spacing: 1.5px;
}

.history-hit {
    color: #00ffc6;
    font-size: 28px;
    font-weight: 950;
    font-family: "Consolas", "Courier New", monospace;
    margin-top: 10px;
}

.eval-label {
    font-size: 13px;
    font-weight: 800;
    opacity: .9;
}

.hit-effect-card {
    background: linear-gradient(135deg, #0f2e29, #11141e);
    border: 1px solid rgba(0,255,198,.35);
    border-radius: 24px;
    padding: 26px;
    box-shadow: 0 0 38px rgba(0,255,198,.20);
    margin: 20px 0 24px;
    text-align: center;
}

.hit-effect-title {
    color: #00ffc6;
    font-size: 34px;
    font-weight: 950;
    text-shadow: 0 0 18px rgba(0,255,198,.45);
    margin-bottom: 8px;
}

.hit-effect-sub {
    color: #f4f6ff;
    font-size: 18px;
    font-weight: 850;
}

.near-effect-card {
    background: linear-gradient(135deg, #2f2713, #11141e);
    border: 1px solid rgba(255,209,102,.35);
    border-radius: 24px;
    padding: 22px;
    box-shadow: 0 0 28px rgba(255,209,102,.16);
    margin: 20px 0 24px;
    text-align: center;
}

.near-effect-title {
    color: #ffd166;
    font-size: 28px;
    font-weight: 950;
    text-shadow: 0 0 14px rgba(255,209,102,.35);
    margin-bottom: 8px;
}

.near-effect-sub {
    color: #f4f6ff;
    font-size: 16px;
    font-weight: 800;
}

.update-panel {
    background: #ffffff;
    border: 1px solid rgba(36,41,56,.12);
    border-radius: 22px;
    padding: 18px 20px;
    box-shadow: 0 14px 34px rgba(15,17,23,.08);
    margin: 16px 0 20px;
}

.update-title {
    font-size: 20px;
    font-weight: 900;
    color: #242938;
    margin-bottom: 6px;
}

.update-note {
    color: #596174;
    font-size: 14px;
    line-height: 1.7;
}

.mainline-section {
    margin-top: 10px;
}

.mainline-label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 7px 13px;
    border-radius: 999px;
    background: rgba(0,255,198,.13);
    color: #00ffc6;
    font-size: 14px;
    font-weight: 950;
    border: 1px solid rgba(0,255,198,.25);
}

.support-label {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    padding: 7px 13px;
    border-radius: 999px;
    background: rgba(255,209,102,.13);
    color: #ffd166;
    font-size: 14px;
    font-weight: 950;
    border: 1px solid rgba(255,209,102,.25);
    margin-top: 18px;
}

.mainline-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 14px;
    margin-top: 14px;
}

.support-grid {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    gap: 12px;
    margin-top: 14px;
}

.pick-chip-main {
    background: linear-gradient(135deg, rgba(0,255,198,.16), rgba(0,255,198,.04));
    border: 1px solid rgba(0,255,198,.32);
    border-radius: 18px;
    padding: 18px 10px;
    text-align: center;
    box-shadow: 0 0 22px rgba(0,255,198,.10);
}

.pick-chip-support {
    background: rgba(255,255,255,.045);
    border: 1px solid rgba(255,209,102,.22);
    border-radius: 16px;
    padding: 14px 8px;
    text-align: center;
}

.pick-rank {
    color: #9ca3af;
    font-size: 12px;
    font-weight: 800;
    margin-bottom: 4px;
}

.pick-num-main {
    color: #00ffc6;
    font-size: 46px;
    line-height: 1;
    font-family: "Consolas", "Courier New", monospace;
    font-weight: 950;
    letter-spacing: 3px;
    text-shadow: 0 0 14px rgba(0,255,198,.35);
}

.pick-num-support {
    color: #ffd166;
    font-size: 34px;
    line-height: 1;
    font-family: "Consolas", "Courier New", monospace;
    font-weight: 950;
    letter-spacing: 2px;
    text-shadow: 0 0 10px rgba(255,209,102,.22);
}

.history-mainline {
    color: #00ffc6;
    font-size: 27px;
    font-weight: 950;
    font-family: "Consolas", "Courier New", monospace;
    letter-spacing: 1.2px;
}

.history-support {
    color: #ffd166;
    font-size: 22px;
    font-weight: 900;
    font-family: "Consolas", "Courier New", monospace;
    letter-spacing: 1px;
    margin-top: 8px;
}


.stats-panel {
    background: #ffffff;
    border: 1px solid rgba(36,41,56,.12);
    border-radius: 22px;
    padding: 22px;
    box-shadow: 0 14px 34px rgba(15,17,23,.08);
    margin: 18px 0 24px;
}

.stats-title {
    font-size: 24px;
    font-weight: 950;
    color: #242938;
    margin-bottom: 8px;
}

.stats-sub {
    color: #596174;
    font-size: 14px;
    font-weight: 700;
    margin-bottom: 16px;
}

.stats-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
}

.stat-card {
    background: linear-gradient(135deg, #171b28, #11141e);
    border-radius: 18px;
    padding: 16px;
    border: 1px solid rgba(255,255,255,.08);
    box-shadow: 0 12px 28px rgba(0,0,0,.20);
}

.stat-label {
    color: #9ca3af;
    font-size: 12px;
    font-weight: 800;
    margin-bottom: 6px;
}

.stat-value {
    color: #f4f6ff;
    font-size: 28px;
    font-weight: 950;
}

.stat-value-hot {
    color: #00ffc6;
    font-size: 28px;
    font-weight: 950;
    text-shadow: 0 0 12px rgba(0,255,198,.28);
}

.stat-value-warm {
    color: #ffd166;
    font-size: 28px;
    font-weight: 950;
    text-shadow: 0 0 12px rgba(255,209,102,.20);
}

.stat-small {
    color: #9ca3af;
    font-size: 12px;
    margin-top: 4px;
    font-weight: 700;
}

.sim-box {
    margin-top: 14px;
    padding: 13px 15px;
    border-radius: 16px;
    background: #f6f7fb;
    color: #596174;
    font-size: 14px;
    font-weight: 750;
    border: 1px solid rgba(36,41,56,.08);
}

.sim-panel {
    background: #ffffff;
    border: 1px solid rgba(36,41,56,.12);
    border-radius: 22px;
    padding: 22px;
    box-shadow: 0 14px 34px rgba(15,17,23,.08);
    margin: 18px 0 24px;
}

.sim-title {
    font-size: 24px;
    font-weight: 950;
    color: #242938;
    margin-bottom: 8px;
}

.sim-sub {
    color: #596174;
    font-size: 14px;
    font-weight: 700;
    margin-bottom: 16px;
}

.sim-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 14px;
}

.sim-missing {
    margin-top: 12px;
    padding: 13px 15px;
    border-radius: 16px;
    background: #fff7e6;
    color: #7a5518;
    font-size: 14px;
    font-weight: 750;
    border: 1px solid rgba(255,209,102,.35);
}

.ai-state-panel {
    background: #ffffff;
    border: 1px solid rgba(36,41,56,.12);
    border-radius: 22px;
    padding: 22px;
    box-shadow: 0 14px 34px rgba(15,17,23,.08);
    margin: 18px 0 24px;
}

.ai-state-title {
    font-size: 24px;
    font-weight: 950;
    color: #242938;
    margin-bottom: 8px;
}

.ai-state-sub {
    color: #596174;
    font-size: 14px;
    font-weight: 700;
    margin-bottom: 16px;
}

.ai-state-grid {
    display: grid;
    grid-template-columns: repeat(2, 1fr);
    gap: 14px;
}

.ai-state-card {
    background: linear-gradient(135deg, #171b28, #11141e);
    border-radius: 18px;
    padding: 18px;
    border: 1px solid rgba(255,255,255,.08);
    box-shadow: 0 12px 28px rgba(0,0,0,.20);
}

.ai-state-label {
    color: #9ca3af;
    font-size: 13px;
    font-weight: 850;
    margin-bottom: 8px;
}

.ai-state-value-hot {
    color: #00ffc6;
    font-size: 34px;
    font-weight: 950;
    text-shadow: 0 0 12px rgba(0,255,198,.32);
}

.ai-state-value-cool {
    color: #74a7ff;
    font-size: 34px;
    font-weight: 950;
    text-shadow: 0 0 12px rgba(116,167,255,.28);
}

.ai-state-value-flat {
    color: #ffd166;
    font-size: 34px;
    font-weight: 950;
    text-shadow: 0 0 12px rgba(255,209,102,.22);
}

.ai-state-small {
    color: #aeb6c7;
    font-size: 13px;
    margin-top: 8px;
    font-weight: 750;
    line-height: 1.6;
}

@media (max-width: 800px) {
    .status-row {
        grid-template-columns: 1fr 1fr;
    }

    .pred-grid {
        grid-template-columns: 1fr;
    }

    .history-grid {
        grid-template-columns: 1fr;
        gap: 22px;
    }

    .stats-grid {
        grid-template-columns: 1fr 1fr;
    }

    .sim-grid {
        grid-template-columns: 1fr 1fr;
    }

    .ai-state-grid {
        grid-template-columns: 1fr;
    }

    .history-head {
        display: block;
        line-height: 1.9;
    }

    .pred-number {
        font-size: 44px;
        letter-spacing: 4px;
    }

    .history-pred {
        font-size: 20px;
    }

    .history-hit {
        font-size: 24px;
    }
}
</style>
""", unsafe_allow_html=True)


# =====================================================
# DBユーティリティ
# =====================================================
def connect_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return row is not None


def ensure_history_table(conn: sqlite3.Connection) -> None:
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {HISTORY_TABLE} (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game TEXT NOT NULL,
        target_round INTEGER NOT NULL,
        target_date TEXT,
        pred_json TEXT NOT NULL,
        actual_number TEXT,
        eval_mark TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(game, target_round)
    )
    """)

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


def find_col(columns: list[str], aliases: list[str]) -> str | None:
    normalized = {c.lower(): c for c in columns}

    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]

    for c in columns:
        low = c.lower()
        for alias in aliases:
            if alias.lower() in low:
                return c

    return None


def normalize_number(value, digits: int) -> str | None:
    if value is None:
        return None

    s = str(value).strip()
    if s in ["", "nan", "None", "NaN", "---", "-"]:
        return None

    s = re.sub(r"\D", "", s)
    if not s:
        return None

    return s.zfill(digits)[-digits:]


def load_draws(conn: sqlite3.Connection, table_name: str, digits: int) -> pd.DataFrame:
    if not table_exists(conn, table_name):
        raise RuntimeError(f"テーブルが見つかりません: {table_name}")

    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    if df.empty:
        raise RuntimeError(f"テーブルが空です: {table_name}")

    cols = list(df.columns)

    round_col = find_col(cols, [
        "round", "draw_round", "draw_no", "draw_number", "kaigou",
        "回号", "抽選回", "抽せん回", "no", "times"
    ])

    date_col = find_col(cols, [
        "date", "draw_date", "抽選日", "抽せん日", "ymd", "日付"
    ])

    number_col = find_col(cols, [
        "winning_number", "winning", "number", "numbers", "result",
        "当選番号", "当せん番号", "本数字", "num", "番号"
    ])

    digit_cols = []
    for i in range(1, digits + 1):
        digit_cols.append(find_col(cols, [
            f"digit_{i}", f"digit{i}", f"d{i}", f"num{i}", f"n{i}"
        ]))

    if round_col is None:
        df["_round_auto"] = range(1, len(df) + 1)
        round_col = "_round_auto"

    if date_col is None:
        df["_date_auto"] = ""
        date_col = "_date_auto"

    if number_col is not None:
        df["_number"] = df[number_col].apply(lambda x: normalize_number(x, digits))
    elif all(c is not None for c in digit_cols):
        df["_number"] = df[digit_cols].astype(str).agg("".join, axis=1)
        df["_number"] = df["_number"].apply(lambda x: normalize_number(x, digits))
    else:
        raise RuntimeError(
            f"{table_name} から当選番号列を判定できません。columns={cols}"
        )

    out = pd.DataFrame({
        "round": pd.to_numeric(df[round_col], errors="coerce"),
        "date": df[date_col].astype(str),
        "number": df["_number"],
    })

    out = out.dropna(subset=["round", "number"]).copy()
    out["number"] = out["number"].astype(str)
    out = out[out["number"].str.fullmatch(r"\d{" + str(digits) + r"}", na=False)].copy()

    if out.empty:
        raise RuntimeError(f"{table_name} から有効な{digits}桁の当選番号を取得できませんでした。")

    out["round"] = out["round"].astype(int)
    out = out.sort_values("round").drop_duplicates("round", keep="last")

    return out


def load_auto_draws(conn: sqlite3.Connection, game: str, digits: int) -> pd.DataFrame:
    if not table_exists(conn, AUTO_DRAW_TABLE):
        return pd.DataFrame(columns=["round", "date", "number", "source"])

    df = pd.read_sql_query(
        f"""
        SELECT draw_round AS round, draw_date AS date, number, source
        FROM {AUTO_DRAW_TABLE}
        WHERE game = ?
        """,
        conn,
        params=(game,),
    )

    if df.empty:
        return pd.DataFrame(columns=["round", "date", "number", "source"])

    df["round"] = pd.to_numeric(df["round"], errors="coerce")
    df["date"] = df["date"].fillna("").astype(str)
    df["number"] = df["number"].apply(lambda x: normalize_number(x, digits))
    df["source"] = df["source"].fillna("auto_draw_results")

    df = df.dropna(subset=["round", "number"]).copy()
    df["round"] = df["round"].astype(int)
    df = df[df["number"].str.fullmatch(r"\d{" + str(digits) + r"}", na=False)].copy()

    return df[["round", "date", "number", "source"]]


def merge_draws(base: pd.DataFrame, auto: pd.DataFrame) -> pd.DataFrame:
    base = base.copy()
    base["source"] = base.get("source", "db")

    merged = pd.concat([base, auto], ignore_index=True)
    if merged.empty:
        return merged

    merged["_priority"] = merged["source"].apply(lambda x: 2 if x == "rakuten" else 1)
    merged = merged.sort_values(["round", "_priority"]).drop_duplicates("round", keep="last")
    merged = merged.drop(columns=["_priority"])
    merged = merged.sort_values("round").reset_index(drop=True)

    return merged


def normalize_rakuten_date(text: str) -> str:
    text = str(text)
    m = re.search(r"(\d{4})[/-](\d{1,2})[/-](\d{1,2})", text)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    return ""


def fetch_url_text(url: str) -> str:
    """
    楽天ページ取得用。
    pandas.read_htmlだけだと環境によって失敗するため、urllibで直接HTMLを取得する。
    """
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

    with urllib.request.urlopen(req, timeout=20) as res:
        raw = res.read()

    # 楽天ページはUTF-8で読める想定。念のためreplace。
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
    楽天×宝くじの当せん番号ページから最新結果を取得。
    HTML直読み + pandas.read_html の二段構え。
    例:
      回号 第6971回
      抽せん日 2026/04/28
      当せん番号 837 / 9678
    """
    url = RAKUTEN_URLS[game]
    rows = []

    # 1) HTML直読みで取得
    try:
        html = fetch_url_text(url)
        text = html_to_text(html)

        pattern = re.compile(
            r"回号\s*第\s*(\d+)\s*回\s*"
            r".{0,120}?"
            r"抽せん日\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2})\s*"
            r".{0,120}?"
            r"当せん番号\s*([0-9]{" + str(digits) + r"})",
            re.DOTALL,
        )

        for m in pattern.finditer(text):
            rows.append({
                "round": int(m.group(1)),
                "date": normalize_rakuten_date(m.group(2)),
                "number": normalize_number(m.group(3), digits),
            })

        # 念のため、回号〜次の回号単位でも解析
        if not rows:
            blocks = re.split(r"(?=回号\s*第\s*\d+\s*回)", text)
            for block in blocks:
                round_match = re.search(r"回号\s*第\s*(\d+)\s*回", block)
                date_match = re.search(r"抽せん日\s*(\d{4}[/-]\d{1,2}[/-]\d{1,2})", block)
                number_match = re.search(r"当せん番号\s*([0-9]{" + str(digits) + r"})", block)

                if round_match and number_match:
                    rows.append({
                        "round": int(round_match.group(1)),
                        "date": normalize_rakuten_date(date_match.group(1)) if date_match else "",
                        "number": normalize_number(number_match.group(1), digits),
                    })

    except Exception:
        pass

    # 2) pandas.read_htmlでも保険取得
    try:
        tables = pd.read_html(url)
    except Exception:
        tables = []

    for df in tables:
        if df.empty:
            continue

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = ["_".join([str(x) for x in col if str(x) != "nan"]) for col in df.columns]
        else:
            df.columns = [str(c) for c in df.columns]

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
                "date": normalize_rakuten_date(m.group(2)),
                "number": normalize_number(m.group(3), digits),
            })

    # 重複削除。大きい回号優先で使う。
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


# =====================================================
# v7予想ロジック
# =====================================================
def build_stats(history_numbers: list[str], digits: int) -> dict:
    long_pos = [Counter() for _ in range(digits)]
    recent_pos = [Counter() for _ in range(digits)]
    total_digit = Counter()
    transition = [defaultdict(Counter) for _ in range(digits)]

    recent = history_numbers[-80:]
    prev = history_numbers[-1] if history_numbers else None

    for num in history_numbers:
        for i, ch in enumerate(num):
            long_pos[i][ch] += 1
            total_digit[ch] += 1

    for num in recent:
        for i, ch in enumerate(num):
            recent_pos[i][ch] += 1

    for a, b in zip(history_numbers[:-1], history_numbers[1:]):
        for i in range(digits):
            transition[i][a[i]][b[i]] += 1

    hist_sums = [
        sum(int(x) for x in n)
        for n in history_numbers[-300:]
        if isinstance(n, str) and re.fullmatch(r"\d+", n)
    ]

    avg_sum = sum(hist_sums) / len(hist_sums) if hist_sums else None
    max_total = max(total_digit.values()) if total_digit else 1

    return {
        "digits": digits,
        "long_pos": long_pos,
        "recent_pos": recent_pos,
        "total_digit": total_digit,
        "transition": transition,
        "prev": prev,
        "avg_sum": avg_sum,
        "max_total": max_total,
    }


def candidate_score_fast(candidate: str, stats: dict) -> float:
    digits = stats["digits"]
    score = 0.0

    long_pos = stats["long_pos"]
    recent_pos = stats["recent_pos"]
    total_digit = stats["total_digit"]
    transition = stats["transition"]
    prev = stats["prev"]
    avg_sum = stats["avg_sum"]
    max_total = stats["max_total"]

    for i, ch in enumerate(candidate):
        score += long_pos[i][ch] * 0.45
        score += recent_pos[i][ch] * 1.20
        score += (max_total - total_digit[ch]) * 0.18

        if prev is not None:
            score += transition[i][prev[i]][ch] * 1.15

    counts = Counter(candidate)

    if digits == 3:
        if 2 in counts.values():
            score += 4.5
        if 3 in counts.values():
            score += 1.2

    if digits == 4:
        if 2 in counts.values():
            score += 4.0
        if 3 in counts.values():
            score += 2.2
        if 4 in counts.values():
            score -= 1.0

    if avg_sum is not None:
        digit_sum = sum(int(x) for x in candidate)
        score -= abs(digit_sum - avg_sum) * 0.22

    return score


@st.cache_data(show_spinner=False)
def generate_v7_predictions_cached(history_numbers_tuple: tuple[str, ...], digits: int, ranks_tuple: tuple[int, ...]) -> list[str]:
    history_numbers = list(history_numbers_tuple)
    ranks = list(ranks_tuple)
    stats = build_stats(history_numbers, digits)

    max_num = 10 ** digits
    scored = []

    for i in range(max_num):
        c = str(i).zfill(digits)
        scored.append((c, candidate_score_fast(c, stats)))

    scored.sort(key=lambda x: (-x[1], x[0]))

    picks = []
    for r in ranks:
        idx = r - 1
        if 0 <= idx < len(scored):
            picks.append(scored[idx][0])

    return picks


# =====================================================
# 評価ロジック
# =====================================================
def judge_prediction(pred_list: list[str], actual: str | None) -> str:
    if actual in [None, "", "---", "-"]:
        return "-"

    hit = str(actual)

    for pred in pred_list:
        pred = str(pred)

        if pred == hit:
            return "◎"

        if sorted(pred) == sorted(hit):
            return "〇"

    best_pos_match = 0
    best_digit_match = 0

    for pred in pred_list:
        pred = str(pred)
        pos_match = sum(1 for a, b in zip(pred, hit) if a == b)

        hit_chars = list(hit)
        digit_match = 0

        for ch in pred:
            if ch in hit_chars:
                digit_match += 1
                hit_chars.remove(ch)

        best_pos_match = max(best_pos_match, pos_match)
        best_digit_match = max(best_digit_match, digit_match)

    if len(hit) == 3 and best_pos_match >= 2:
        return "▲"

    if len(hit) == 4 and best_pos_match >= 3:
        return "▲"

    if best_digit_match >= 2:
        return "△"

    if best_digit_match >= 1:
        return "※"

    return "×"


def eval_color(mark: str) -> str:
    return {
        "◎": "#00ffc6",
        "〇": "#5ee7ff",
        "▲": "#ffd166",
        "△": "#ff9f43",
        "※": "#b8b8ff",
        "×": "#777777",
        "-": "#999999",
    }.get(mark, "#ffffff")


def eval_label(mark: str) -> str:
    return {
        "◎": "ストレート的中",
        "〇": "ボックス的中",
        "▲": "位置一致",
        "△": "数字一致",
        "※": "一部一致",
        "×": "該当なし",
        "-": "未抽選",
    }.get(mark, "")


# =====================================================
# 履歴保存・取得
# =====================================================
def fetch_saved_prediction(conn: sqlite3.Connection, game: str, target_round: int) -> list[str] | None:
    row = conn.execute(f"""
    SELECT pred_json
    FROM {HISTORY_TABLE}
    WHERE game = ? AND target_round = ?
    """, (game, target_round)).fetchone()

    if row is None:
        return None

    try:
        preds = json.loads(row["pred_json"])
        if isinstance(preds, list) and preds:
            return [str(x) for x in preds]
    except Exception:
        return None

    return None


def insert_prediction(conn: sqlite3.Connection, game: str, target_round: int, target_date: str, preds: list[str]) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    pred_json = json.dumps(preds, ensure_ascii=False)

    conn.execute(f"""
    INSERT OR IGNORE INTO {HISTORY_TABLE}
    (game, target_round, target_date, pred_json, actual_number, eval_mark, created_at, updated_at)
    VALUES (?, ?, ?, ?, NULL, NULL, ?, ?)
    """, (game, target_round, target_date, pred_json, now, now))

    conn.commit()


def get_or_create_prediction(
    conn: sqlite3.Connection,
    game: str,
    target_round: int,
    target_date: str,
    history_numbers: list[str],
    digits: int,
    ranks: list[int],
) -> list[str]:
    saved = fetch_saved_prediction(conn, game, target_round)

    if saved is not None:
        return saved

    preds = generate_v7_predictions_cached(tuple(history_numbers), digits, tuple(ranks))
    insert_prediction(conn, game, target_round, target_date, preds)
    return preds


def update_actuals(conn: sqlite3.Connection, game: str, draws: pd.DataFrame) -> None:
    rows = conn.execute(f"""
    SELECT id, target_round, pred_json
    FROM {HISTORY_TABLE}
    WHERE game = ?
    """, (game,)).fetchall()

    draw_map = {
        int(r["round"]): r["number"]
        for _, r in draws.iterrows()
    }

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for row in rows:
        target_round = int(row["target_round"])

        if target_round not in draw_map:
            continue

        actual = draw_map[target_round]
        preds = json.loads(row["pred_json"])
        mark = judge_prediction(preds, actual)

        conn.execute(f"""
        UPDATE {HISTORY_TABLE}
        SET actual_number = ?, eval_mark = ?, updated_at = ?
        WHERE id = ?
        """, (actual, mark, now, row["id"]))

    conn.commit()


def load_prediction_history(conn: sqlite3.Connection) -> pd.DataFrame:
    if not table_exists(conn, HISTORY_TABLE):
        return pd.DataFrame()

    return pd.read_sql_query(f"""
    SELECT *
    FROM {HISTORY_TABLE}
    ORDER BY target_round DESC, game ASC
    """, conn)


def build_combined_history(
    hist_df: pd.DataFrame,
    draws_n3: pd.DataFrame,
    draws_n4: pd.DataFrame,
    target_round: int,
    limit: int = HISTORY_LIMIT,
) -> list[dict]:
    """
    履歴表示用。
    prediction_history_v7 に予想がある回だけでなく、
    直近の抽選済み回も並べることで
    「第6972回の次に第6941回が出る」ような飛びを防ぐ。

    予想がない回は「予想：記録なし」として表示。
    """
    hist_map = {}

    if hist_df is not None and not hist_df.empty:
        for _, r in hist_df.iterrows():
            round_no = int(r["target_round"])
            if round_no not in hist_map:
                hist_map[round_no] = {
                    "round": round_no,
                    "date": "",
                    "n3_pred": [],
                    "n3_hit": "---",
                    "n3_eval": "-",
                    "n4_pred": [],
                    "n4_hit": "---",
                    "n4_eval": "-",
                }

            item = hist_map[round_no]
            game = r["game"]
            preds = json.loads(r["pred_json"])
            actual = r["actual_number"] if pd.notna(r["actual_number"]) else "---"
            mark = r["eval_mark"] if pd.notna(r["eval_mark"]) else judge_prediction(preds, actual)

            if r["target_date"]:
                item["date"] = str(r["target_date"])

            if game == "N3":
                item["n3_pred"] = preds
                item["n3_hit"] = actual
                item["n3_eval"] = mark

            if game == "N4":
                item["n4_pred"] = preds
                item["n4_hit"] = actual
                item["n4_eval"] = mark

    n3_map = {}
    if draws_n3 is not None and not draws_n3.empty:
        for _, r in draws_n3.iterrows():
            n3_map[int(r["round"])] = {
                "date": str(r.get("date", "")),
                "number": str(r.get("number", "---")),
            }

    n4_map = {}
    if draws_n4 is not None and not draws_n4.empty:
        for _, r in draws_n4.iterrows():
            n4_map[int(r["round"])] = {
                "date": str(r.get("date", "")),
                "number": str(r.get("number", "---")),
            }

    max_round = max(
        [target_round]
        + list(hist_map.keys())
        + list(n3_map.keys())
        + list(n4_map.keys())
    )

    min_round = max(1, max_round - limit + 1)
    items = []

    for round_no in range(max_round, min_round - 1, -1):
        item = hist_map.get(round_no, {
            "round": round_no,
            "date": "",
            "n3_pred": [],
            "n3_hit": "---",
            "n3_eval": "-",
            "n4_pred": [],
            "n4_hit": "---",
            "n4_eval": "-",
        })

        # 実当選データを反映
        if round_no in n3_map:
            item["n3_hit"] = n3_map[round_no]["number"]
            if not item["date"]:
                item["date"] = n3_map[round_no]["date"]
            if item["n3_pred"]:
                item["n3_eval"] = judge_prediction(item["n3_pred"], item["n3_hit"])

        if round_no in n4_map:
            item["n4_hit"] = n4_map[round_no]["number"]
            if not item["date"]:
                item["date"] = n4_map[round_no]["date"]
            if item["n4_pred"]:
                item["n4_eval"] = judge_prediction(item["n4_pred"], item["n4_hit"])

        # 予想も実績もない完全な空行は出さない
        has_any = (
            item["n3_pred"] or item["n4_pred"]
            or item["n3_hit"] not in ["---", "-", "", None]
            or item["n4_hit"] not in ["---", "-", "", None]
            or round_no == target_round
        )

        if has_any:
            if not item["date"]:
                item["date"] = "未確定"
            items.append(item)

    return items[:limit]



# =====================================================
# 的中演出
# =====================================================
def find_latest_evaluated_hit(combined_history: list[dict]) -> dict | None:
    """
    最新の評価済み履歴から演出対象を探す。
    ◎ or 〇 があれば強演出。
    ▲ or △ は軽い惜しい演出。
    ※ / × / - は演出なし。
    """
    for h in combined_history:
        n3_eval = h.get("n3_eval", "-")
        n4_eval = h.get("n4_eval", "-")

        # 未発表回はスキップ
        if n3_eval == "-" and n4_eval == "-":
            continue

        if n3_eval in ["◎", "〇"] or n4_eval in ["◎", "〇"]:
            return {
                "level": "hit",
                "round": h.get("round"),
                "date": h.get("date", ""),
                "n3_eval": n3_eval,
                "n4_eval": n4_eval,
                "n3_hit": h.get("n3_hit", "---"),
                "n4_hit": h.get("n4_hit", "---"),
            }

        if n3_eval in ["▲", "△"] or n4_eval in ["▲", "△"]:
            return {
                "level": "near",
                "round": h.get("round"),
                "date": h.get("date", ""),
                "n3_eval": n3_eval,
                "n4_eval": n4_eval,
                "n3_hit": h.get("n3_hit", "---"),
                "n4_hit": h.get("n4_hit", "---"),
            }

    return None


def render_hit_effect(effect: dict | None) -> str:
    if effect is None:
        return ""

    round_no = effect.get("round", "")
    date = effect.get("date", "")
    n3_eval = effect.get("n3_eval", "-")
    n4_eval = effect.get("n4_eval", "-")
    n3_hit = display_hit(effect.get("n3_hit", "---"))
    n4_hit = display_hit(effect.get("n4_hit", "---"))

    if effect.get("level") == "hit":
        return f"""
<div class="hit-effect-card">
    <div class="hit-effect-title">🎉 的中あり！ 🎉</div>
    <div class="hit-effect-sub">
        第{round_no}回 {date}<br>
        N3：{n3_hit} / 評価 {n3_eval}　｜　N4：{n4_hit} / 評価 {n4_eval}
    </div>
</div>
"""

    if effect.get("level") == "near":
        return f"""
<div class="near-effect-card">
    <div class="near-effect-title">✨ 惜しい反応あり ✨</div>
    <div class="near-effect-sub">
        第{round_no}回 {date}<br>
        N3：{n3_hit} / 評価 {n3_eval}　｜　N4：{n4_hit} / 評価 {n4_eval}
    </div>
</div>
"""

    return ""



# =====================================================
# 勝率可視化
# =====================================================
def calc_eval_stats(combined_history: list[dict], key_prefix: str) -> dict:
    eval_key = f"{key_prefix}_eval"

    evaluated = [
        h.get(eval_key, "-")
        for h in combined_history
        if h.get(eval_key, "-") not in ["-", "", None]
    ]

    total = len(evaluated)

    if total == 0:
        return {
            "total": 0,
            "straight": 0,
            "box": 0,
            "effective": 0,
            "straight_rate": None,
            "hit_rate": None,
            "effective_rate": None,
        }

    straight = sum(1 for x in evaluated if x == "◎")
    box = sum(1 for x in evaluated if x in ["◎", "〇"])
    effective = sum(1 for x in evaluated if x in ["◎", "〇", "▲", "△"])

    return {
        "total": total,
        "straight": straight,
        "box": box,
        "effective": effective,
        "straight_rate": straight / total,
        "hit_rate": box / total,
        "effective_rate": effective / total,
    }


def fmt_rate(value) -> str:
    if value is None:
        return "集計中"
    return f"{value * 100:.1f}%"



def normalize_hit_type(value) -> str:
    s = str(value).strip().lower()

    if s in ["straight", "ストレート", "◎"]:
        return "straight"

    if s in ["box", "ボックス", "〇", "○"]:
        return "box"

    if s in ["near", "position", "▲"]:
        return "near"

    if s in ["partial", "△", "※"]:
        return "partial"

    return "none"


def calc_sim_stats(path: str) -> dict | None:
    csv_path = Path(path)

    if not csv_path.exists():
        return None

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            return None

    if df.empty:
        return None

    cols = list(df.columns)

    hit_col = None
    for c in cols:
        low = str(c).lower()
        if low in ["hit_type", "eval", "evaluation", "判定", "評価", "result"]:
            hit_col = c
            break

    if hit_col is None:
        return None

    hit_types = df[hit_col].apply(normalize_hit_type)
    total = len(hit_types)

    # 1行あたりの予想数（" / "区切り想定）
    picks_per_row = 1
    if "predictions" in df.columns:
        try:
            picks_per_row = int(df["predictions"].astype(str).iloc[0].count("/") + 1)
        except Exception:
            picks_per_row = 1

    total_picks = total * picks_per_row

    straight = int((hit_types == "straight").sum())
    box_only = int((hit_types == "box").sum())
    box_total = straight + box_only
    effective = int(hit_types.isin(["straight", "box", "near", "partial"]).sum())

    return {
        "total": total,
        "straight": straight,
        "box_total": box_total,
        "effective": effective,
        "straight_rate": straight / total if total else None,
        "box_rate": box_total / total if total else None,
         "effective_rate": effective / total if total else None,
        "total_picks": total_picks,
        "picks_per_row": picks_per_row,
        "straight_rate_pick": (straight / total_picks) if total_picks else None,
        "box_rate_pick": (box_total / total_picks) if total_picks else None,
    }


def render_simulation_dashboard() -> str:
    n3 = calc_sim_stats(SIM_N3_CSV)
    n4 = calc_sim_stats(SIM_N4_CSV)

    if n3 is None and n4 is None:
        return (
            '<div class="sim-panel">'
            '<div class="sim-title">📈 シミュレーション成績</div>'
            '<div class="sim-sub">過去検証CSVを置くと、ここに参考成績を表示します。</div>'
            '<div class="sim-missing">'
            f'CSV未検出：{SIM_N3_CSV} / {SIM_N4_CSV}<br>'
            '同じフォルダに配置すると自動で読み込みます。必要列は hit_type です。'
            '</div>'
            '</div>'
        )

    def card(title: str, value: str, small: str, hot: bool = True) -> str:
        cls = "stat-value-hot" if hot else "stat-value-warm"
        return (
            '<div class="stat-card">'
            f'<div class="stat-label">{title}</div>'
            f'<div class="{cls}">{value}</div>'
            f'<div class="stat-small">{small}</div>'
            '</div>'
        )

    def safe_rate(stats, key):
        if stats is None:
            return "未接続"
        return fmt_rate(stats[key])

    def safe_small(stats, count_key):
        if stats is None:
            return "CSVなし"
        return f'{stats[count_key]} / {stats["total"]}回\n({stats[count_key]} / {stats["total_picks"]}予想)'

    html = (
        '<div class="sim-panel">'
        '<div class="sim-title">📈 シミュレーション成績</div>'
        '<div class="sim-sub">'
        '過去検証CSVから読み込んだ参考成績です。実運用成績とは分けて表示します。'
        '</div>'
        '<div class="sim-grid">'
        + card("N3 ストレート率", safe_rate(n3, "straight_rate"), safe_small(n3, "straight"), True)
        + card("N3 BOX率（◎＋〇）", safe_rate(n3, "box_rate"), safe_small(n3, "box_total"), False)
        + card("N4 ストレート率", safe_rate(n4, "straight_rate"), safe_small(n4, "straight"), True)
        + card("N4 BOX率（◎＋〇）", safe_rate(n4, "box_rate"), safe_small(n4, "box_total"), False)
        + '</div>'
        '</div>'
    )

    return html


def render_winrate_dashboard(combined_history: list[dict]) -> str:
    """
    実運用成績パネル。
    重要：Streamlitのmarkdownはインデント付きHTMLをコード扱いすることがあるため、
    すべて行頭空白なしの文字列連結で返す。
    """
    n3 = calc_eval_stats(combined_history, "n3")
    n4 = calc_eval_stats(combined_history, "n4")

    return (
        '<div class="stats-panel">'
        '<div class="stats-title">📊 実運用成績</div>'
        '<div class="stats-sub">'
        'prediction_history_v7 に保存された予想履歴から自動集計しています。未発表回は集計対象外です。'
        '</div>'
        '<div class="stats-grid">'
        '<div class="stat-card">'
        '<div class="stat-label">N3 的中率（◎＋〇）</div>'
        f'<div class="stat-value-hot">{fmt_rate(n3["hit_rate"])}</div>'
        f'<div class="stat-small">対象 {n3["total"]} 回</div>'
        '</div>'
        '<div class="stat-card">'
        '<div class="stat-label">N3 有効反応（▲以上）</div>'
        f'<div class="stat-value-warm">{fmt_rate(n3["effective_rate"])}</div>'
        f'<div class="stat-small">◎ {n3["straight"]} / ◎〇 {n3["box"]}</div>'
        '</div>'
        '<div class="stat-card">'
        '<div class="stat-label">N4 的中率（◎＋〇）</div>'
        f'<div class="stat-value-hot">{fmt_rate(n4["hit_rate"])}</div>'
        f'<div class="stat-small">対象 {n4["total"]} 回</div>'
        '</div>'
        '<div class="stat-card">'
        '<div class="stat-label">N4 有効反応（▲以上）</div>'
        f'<div class="stat-value-warm">{fmt_rate(n4["effective_rate"])}</div>'
        f'<div class="stat-small">◎ {n4["straight"]} / ◎〇 {n4["box"]}</div>'
        '</div>'
        '</div>'
        ''
        '</div>'
    )



# =====================================================
# AI状態トレンド判定
# =====================================================
def eval_score_from_value(value) -> int:
    s = str(value).strip().lower()

    # mark列優先想定
    if s in ["◎", "straight"]:
        return 5
    if s in ["〇", "○", "box"]:
        return 4
    if s in ["▲", "near", "position"]:
        return 3
    if s in ["△", "partial"]:
        return 2
    if s in ["※", "one"]:
        return 1

    return 0


def load_recent_scores_from_sim(path: str, window: int = 20) -> dict:
    csv_path = Path(path)

    if not csv_path.exists():
        return {
            "available": False,
            "state": "データ不足",
            "class": "flat",
            "before_avg": None,
            "after_avg": None,
            "delta": None,
            "scores": [],
        }

    try:
        df = pd.read_csv(csv_path, encoding="utf-8-sig")
    except Exception:
        try:
            df = pd.read_csv(csv_path)
        except Exception:
            return {
                "available": False,
                "state": "データ不足",
                "class": "flat",
                "before_avg": None,
                "after_avg": None,
                "delta": None,
                "scores": [],
            }

    if df.empty:
        return {
            "available": False,
            "state": "データ不足",
            "class": "flat",
            "before_avg": None,
            "after_avg": None,
            "delta": None,
            "scores": [],
        }

    score_col = None
    for c in ["mark", "eval", "evaluation", "評価", "hit_type", "result"]:
        if c in df.columns:
            score_col = c
            break

    if score_col is None:
        return {
            "available": False,
            "state": "データ不足",
            "class": "flat",
            "before_avg": None,
            "after_avg": None,
            "delta": None,
            "scores": [],
        }

    recent = df.tail(window).copy()
    scores = recent[score_col].apply(eval_score_from_value).tolist()

    if len(scores) < 10:
        return {
            "available": False,
            "state": "データ不足",
            "class": "flat",
            "before_avg": None,
            "after_avg": None,
            "delta": None,
            "scores": scores,
        }

    half = len(scores) // 2
    before = scores[:half]
    after = scores[half:]

    before_avg = sum(before) / len(before) if before else 0
    after_avg = sum(after) / len(after) if after else 0
    delta = after_avg - before_avg

    if delta >= 0.30:
        state = "🔥 ホット"
        cls = "hot"
    elif delta <= -0.30:
        state = "❄️ クール"
        cls = "cool"
    else:
        state = "⚖️ フラット"
        cls = "flat"

    return {
        "available": True,
        "state": state,
        "class": cls,
        "before_avg": before_avg,
        "after_avg": after_avg,
        "delta": delta,
        "scores": scores,
    }


def fmt_score(value) -> str:
    if value is None:
        return "-"
    return f"{value:.2f}"


def render_ai_trend_dashboard() -> str:
    n3 = load_recent_scores_from_sim(SIM_N3_CSV, window=20)
    n4 = load_recent_scores_from_sim(SIM_N4_CSV, window=20)

    def card(label: str, data: dict) -> str:
        cls = data.get("class", "flat")
        value_cls = {
            "hot": "ai-state-value-hot",
            "cool": "ai-state-value-cool",
            "flat": "ai-state-value-flat",
        }.get(cls, "ai-state-value-flat")

        before_avg = fmt_score(data.get("before_avg"))
        after_avg = fmt_score(data.get("after_avg"))
        delta = fmt_score(data.get("delta"))

        if not data.get("available"):
            small = "直近データが不足しています。"
        else:
            small = f"前半平均 {before_avg} → 後半平均 {after_avg}<br>変化量 {delta} / 直近20回"

        return (
            '<div class="ai-state-card">'
            f'<div class="ai-state-label">{label}</div>'
            f'<div class="{value_cls}">{data.get("state", "データ不足")}</div>'
            f'<div class="ai-state-small">{small}</div>'
            '</div>'
        )

    return (
        '<div class="ai-state-panel">'
        '<div class="ai-state-title">🎯 AI状態トレンド</div>'
        '<div class="ai-state-sub">'
        '◎=5点、〇=4点、▲=3点、△=2点、※=1点、×=0点として直近20回の前半/後半を比較します。'
        '</div>'
        '<div class="ai-state-grid">'
        + card("Numbers3", n3)
        + card("Numbers4", n4)
        + '</div>'
        '</div>'
    )


# =====================================================
# HTML部品
# =====================================================
def render_prediction_card(title: str, preds: list[str], ranks: list[int]) -> str:
    """
    本線UIカード。
    重要：Streamlitのmarkdownは行頭に空白が多いHTMLをコードブロック扱いすることがあるため、
    HTMLは行頭空白なしで組み立てる。
    """
    main_preds = preds[:2]
    support_preds = preds[2:]

    main_parts = []
    for i, n in enumerate(main_preds):
        main_parts.append(
            '<div class="pick-chip-main">'
            f'<div class="pick-rank">本線 {i + 1}</div>'
            f'<div class="pick-num-main">{n}</div>'
            '</div>'
        )

    support_parts = []
    for i, n in enumerate(support_preds):
        support_parts.append(
            '<div class="pick-chip-support">'
            f'<div class="pick-rank">抑え {i + 1}</div>'
            f'<div class="pick-num-support">{n}</div>'
            '</div>'
        )

    main_html = "".join(main_parts)
    support_html = "".join(support_parts)

    return (
        '<div class="pred-card">'
        f'<div class="main-card-title">{title}</div>'
        '<div class="mainline-section">'
        '<div class="mainline-label">🔥 本線</div>'
        f'<div class="mainline-grid">{main_html}</div>'
        '</div>'
        '<div class="mainline-section">'
        '<div class="support-label">○ 抑え</div>'
        f'<div class="support-grid">{support_html}</div>'
        '</div>'
        f'<div class="rank-text">AI最適順位：{", ".join(map(str, ranks))}</div>'
        '</div>'
    )


def display_hit(value) -> str:
    if value in [None, "", "---", "-"]:
        return "未発表"
    return str(value)


def render_history_card(h: dict) -> str:
    n3_color = eval_color(h["n3_eval"])
    n4_color = eval_color(h["n4_eval"])

    if h["n3_pred"]:
        n3_main_text = " / ".join(h["n3_pred"][:2])
        n3_support_text = " / ".join(h["n3_pred"][2:])
    else:
        n3_main_text = "記録なし"
        n3_support_text = ""

    if h["n4_pred"]:
        n4_main_text = " / ".join(h["n4_pred"][:2])
        n4_support_text = " / ".join(h["n4_pred"][2:])
    else:
        n4_main_text = "記録なし"
        n4_support_text = ""

    html = f"""
<div class="history-card">
    <div class="history-head">
        <div>第{h["round"]}回 ｜ {h["date"]}</div>
        <div>
            N3評価：
            <span style="color:{n3_color};font-size:24px;font-weight:950;">{h["n3_eval"]}</span>
            <span class="eval-label" style="color:{n3_color};"> {eval_label(h["n3_eval"])}</span>
            &nbsp;&nbsp;
            N4評価：
            <span style="color:{n4_color};font-size:24px;font-weight:950;">{h["n4_eval"]}</span>
            <span class="eval-label" style="color:{n4_color};"> {eval_label(h["n4_eval"])}</span>
        </div>
    </div>
    <div class="history-grid">
        <div>
            <div class="history-label">Numbers3</div>
            <div class="history-mainline">🔥 本線：{n3_main_text}</div>
            <div class="history-support">○ 抑え：{n3_support_text}</div>
            <div class="history-hit">当選：{display_hit(h["n3_hit"])}</div>
        </div>
        <div>
            <div class="history-label">Numbers4</div>
            <div class="history-mainline">🔥 本線：{n4_main_text}</div>
            <div class="history-support">○ 抑え：{n4_support_text}</div>
            <div class="history-hit">当選：{display_hit(h["n4_hit"])}</div>
        </div>
    </div>
</div>
"""
    return textwrap.dedent(html).strip()


# =====================================================
# 画面本体
# =====================================================
db_path = DEFAULT_DB_PATH
today = datetime.now().strftime("%Y-%m-%d")

st.markdown(f"""
<div class="app-title">{APP_TITLE}</div>
<div class="app-sub">{today} / 最新予想</div>
""", unsafe_allow_html=True)

db_file = Path(db_path)

if not db_file.exists():
    st.error(f"numbers.db が見つかりません: {db_file.resolve()}")
    st.info("この .py ファイルと同じフォルダに numbers.db を置いてから再実行してください。")
    st.stop()

latest_check_message = "楽天最新結果チェック：未実行"

try:
    with st.spinner("numbers.dbを読み込み中..."):
        conn = connect_db(str(db_file))
        ensure_history_table(conn)

    with st.spinner("楽天×宝くじで最新抽選結果を確認中..."):
        fetched_messages = []
        for game, digits in [("N3", 3), ("N4", 4)]:
            rows = parse_rakuten_latest(game, digits)
            save_auto_draws(conn, game, rows)
            if rows:
                latest = max(rows, key=lambda x: int(x["round"]))
                fetched_messages.append(
                    f'{game}: 第{latest["round"]}回 {latest.get("date", "")} / {latest["number"]}'
                )
            else:
                fetched_messages.append(f"{game}: 取得できませんでした")

        latest_check_message = "楽天最新結果チェック：" + " ｜ ".join(fetched_messages)

    with st.spinner("抽選データを統合中..."):
        base_n3 = load_draws(conn, TABLE_N3, 3)
        base_n4 = load_draws(conn, TABLE_N4, 4)

        auto_n3 = load_auto_draws(conn, "N3", 3)
        auto_n4 = load_auto_draws(conn, "N4", 4)

        draws_n3 = merge_draws(base_n3, auto_n3)
        draws_n4 = merge_draws(base_n4, auto_n4)

        latest_n3_round = int(draws_n3["round"].max())
        latest_n4_round = int(draws_n4["round"].max())
        target_round = max(latest_n3_round, latest_n4_round) + 1
        target_date = today

        n3_history_numbers = draws_n3["number"].dropna().astype(str).tolist()
        n4_history_numbers = draws_n4["number"].dropna().astype(str).tolist()

    with st.spinner("予想ロジックを実行中..."):
        preds_n3 = get_or_create_prediction(
            conn=conn,
            game="N3",
            target_round=target_round,
            target_date=target_date,
            history_numbers=n3_history_numbers,
            digits=3,
            ranks=RANK_N3,
        )

        preds_n4 = get_or_create_prediction(
            conn=conn,
            game="N4",
            target_round=target_round,
            target_date=target_date,
            history_numbers=n4_history_numbers,
            digits=4,
            ranks=RANK_N4,
        )

    with st.spinner("予想履歴を更新中..."):
        update_actuals(conn, "N3", draws_n3)
        update_actuals(conn, "N4", draws_n4)

        hist_df = load_prediction_history(conn)
        combined_history = build_combined_history(hist_df, draws_n3, draws_n4, target_round, HISTORY_LIMIT)

except Exception as e:
    st.error("DB連携中にエラーが発生しました。")
    st.exception(e)
    st.stop()


st.markdown(f'<div class="logic-card" style="padding:14px 18px;margin-top:10px;margin-bottom:14px;"><div class="logic-text" style="font-size:14px;">{latest_check_message}</div></div>', unsafe_allow_html=True)

st.markdown("""
<div class="update-panel">
    <div class="update-title">⚙️ 運用メニュー</div>
    <div class="update-note">
        最新結果をもう一度確認したい時や、画面表示を更新したい時に使います。<br>
        同じ抽選回の予想はDB保存済みの内容を使うため、ボタンを押しても予想はブレません。
    </div>
</div>
""", unsafe_allow_html=True)

col_refresh, col_reload, col_spacer = st.columns([1, 1, 3])

with col_refresh:
    if st.button("🔄 最新結果を取得", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

with col_reload:
    if st.button("🎯 予想を再読み込み", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

tab1, tab2, tab3 = st.tabs(["🎯 今日の予想", "🧠 ロジック", "📜 履歴"])


# =====================================================
# 今日の予想
# =====================================================
with tab1:
    hit_effect = find_latest_evaluated_hit(combined_history)

    if hit_effect and hit_effect.get("level") == "hit":
        st.balloons()

    effect_html = render_hit_effect(hit_effect)
    if effect_html:
        st.markdown(textwrap.dedent(effect_html).strip(), unsafe_allow_html=True)

    st.markdown(f"""
<div class="status-row">
    <div class="status-box">
        <div class="status-label">最新N3回号</div>
        <div class="status-value">第{latest_n3_round}回</div>
    </div>
    <div class="status-box">
        <div class="status-label">最新N4回号</div>
        <div class="status-value">第{latest_n4_round}回</div>
    </div>
    <div class="status-box">
        <div class="status-label">予想対象</div>
        <div class="status-value">第{target_round}回</div>
    </div>
    <div class="status-box">
        <div class="status-label">DB履歴</div>
        <div class="status-value">固定保存</div>
    </div>
</div>
""", unsafe_allow_html=True)

    st.markdown(render_winrate_dashboard(combined_history), unsafe_allow_html=True)
    st.markdown(render_simulation_dashboard(), unsafe_allow_html=True)
    st.markdown(render_ai_trend_dashboard(), unsafe_allow_html=True)

    html = (
        '<div class="pred-grid">'
        + render_prediction_card("ナンバーズ3", preds_n3, RANK_N3)
        + render_prediction_card("ナンバーズ4", preds_n4, RANK_N4)
        + '</div>'
    )
    st.markdown(html, unsafe_allow_html=True)



# =====================================================
# ロジック
# =====================================================
with tab2:
    logic_html = f"""
<div class="logic-card">
    <div class="logic-title">🧠 v7予想ロジック</div>
    <div class="logic-text">
        <b>v7は、過去検証で採用した固定順位モデルを使います。</b><br><br>
        Numbers3：AI最適順位 <b>{", ".join(map(str, RANK_N3))}</b><br>
        Numbers4：AI最適順位 <b>{", ".join(map(str, RANK_N4))}</b><br><br>
        候補数字をスコア順に並べ、指定順位の候補を採用します。<br>表示では上位2つを <b>🔥本線</b>、残り3つを <b>○抑え</b> として分けています。<br>実運用成績は、予想履歴テーブルから未発表回を除外して自動集計します。<br>シミュレーション成績は、同じフォルダに置いた sim_numbers3.csv / sim_numbers4.csv から読み込みます。<br>AI状態トレンドは、直近20回の評価点の変化からホット/クール/フラットを判定します。<br>公開版では安全性を優先し、メール送信機能は外しています。<br>
        スコアには、長期出現率、直近80回の勢い、冷え数字補正、前回数字からの遷移、
        合計値バランス、ダブル傾向を反映しています。<br><br>
        <b>重要：</b>一度保存された予想は、同じ抽選回であれば再計算せずDB内の予想を表示します。<br>
        そのため、日付が変わっても「第{target_round}回」の予想はブレません。<br><br>
        起動時に楽天×宝くじを確認し、最新結果を <b>{AUTO_DRAW_TABLE}</b> に保存します。<br>その後、既存DBと統合して最新回を判定し、予想ロジックを動かします。
    </div>
</div>
"""
    st.markdown(textwrap.dedent(logic_html).strip(), unsafe_allow_html=True)


# =====================================================
# 履歴
# =====================================================
with tab3:
    st.markdown('<div class="history-title">📜 予想履歴</div>', unsafe_allow_html=True)

    if not combined_history:
        st.info("まだ予想履歴がありません。今日の予想を保存するとここに表示されます。")
    else:
        for h in combined_history:
            st.markdown(render_history_card(h), unsafe_allow_html=True)

        rows = []
        for h in combined_history:
            rows.append({
                "抽選回": f'第{h["round"]}回',
                "抽選日": h["date"],
                "N3予想": " / ".join(h["n3_pred"]) if h["n3_pred"] else "記録なし",
                "N3当選": display_hit(h["n3_hit"]),
                "N3評価": h["n3_eval"],
                "N3評価コメント": eval_label(h["n3_eval"]),
                "N4予想": " / ".join(h["n4_pred"]) if h["n4_pred"] else "記録なし",
                "N4当選": display_hit(h["n4_hit"]),
                "N4評価": h["n4_eval"],
                "N4評価コメント": eval_label(h["n4_eval"]),
            })

        csv_df = pd.DataFrame(rows)
        csv = csv_df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

        st.download_button(
            label="📥 履歴CSVダウンロード",
            data=csv,
            file_name="numbers_v7_history.csv",
            mime="text/csv"
        )
