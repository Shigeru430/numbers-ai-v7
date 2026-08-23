from numbers_ai_v8_hybrid_core import generate_hybrid_predictions
# Numbers AI v8 Hybrid Production Overlay v2
# build_sim_numbers_v7.py
# ============================================================
# Numbers AI v7 シミュレーションCSV自動生成スクリプト
#
# 目的:
#   numbers.db の過去データから、app側で読む
#   sim_numbers3.csv / sim_numbers4.csv を自動生成する。
#
# 出力:
#   sim_numbers3.csv
#   sim_numbers4.csv
#
# 判定 hit_type:
#   straight : ストレート的中（◎）
#   box      : ボックス的中（〇）
#   near     : 位置一致レベル（▲）
#   partial  : 数字一致レベル（△/※）
#   none     : 該当なし（×）
#
# 実行例:
#   py -3.14-64 build_sim_numbers_v7.py --db-path numbers.db --start-after 500 --last-n 1000
# ============================================================

import argparse
import itertools
import re
import math
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

import pandas as pd
import numpy as np


TABLE_N3 = "numbers3_enriched"
TABLE_N4 = "numbers4_enriched"
AUTO_DRAW_TABLE = "auto_draw_results"

RANK_N3 = [4, 5, 9, 11, 20]
RANK_N4 = [3, 6, 7, 9, 17]


# =====================================================
# DB読み込み
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


def find_col(columns: list[str], aliases: list[str]) -> str | None:
    normalized = {str(c).lower(): c for c in columns}

    for alias in aliases:
        if alias.lower() in normalized:
            return normalized[alias.lower()]

    for c in columns:
        low = str(c).lower()
        for alias in aliases:
            if alias.lower() in low:
                return c

    return None


def normalize_number(value, digits: int) -> str | None:
    """Normalize Numbers values safely. Integer-like floats such as 413.0 become 413, not 4130."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        s = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            return None
        s = str(int(value))
    else:
        s = str(value).strip()
        if s in ["", "nan", "None", "NaN", "---", "-"]:
            return None
        m = re.fullmatch(r"([+-]?\d+)\.0+", s)
        if m:
            s = m.group(1)
        if not re.fullmatch(r"[+-]?\d+", s):
            return None
    s = s.lstrip("+")
    if s.startswith("-") or len(s) > digits:
        return None
    return s.zfill(digits)

def normalize_round(value) -> int | None:
    if value is None:
        return None

    s = re.sub(r"\D", "", str(value))
    if not s:
        return None

    try:
        return int(s)
    except Exception:
        return None


def normalize_date(value) -> str:
    if value is None:
        return ""

    s = str(value).strip()
    if not s or s.lower() == "nan":
        return ""

    m = re.search(r"(\d{4})\D+(\d{1,2})\D+(\d{1,2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    m = re.search(r"(\d{4})(\d{2})(\d{2})", s)
    if m:
        y, mo, d = m.groups()
        return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    return s


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
        "round": df[round_col].apply(normalize_round),
        "date": df[date_col].apply(normalize_date),
        "number": df["_number"],
    })

    out = out.dropna(subset=["round", "number"]).copy()
    out["number"] = out["number"].astype(str)
    out = out[out["number"].str.fullmatch(r"\d{" + str(digits) + r"}", na=False)].copy()

    if out.empty:
        raise RuntimeError(f"{table_name} から有効な{digits}桁の当選番号を取得できませんでした。")

    out["round"] = out["round"].astype(int)
    out = out.sort_values("round").drop_duplicates("round", keep="last").reset_index(drop=True)

    return out


def load_auto_draws(conn: sqlite3.Connection, game: str, digits: int) -> pd.DataFrame:
    if not table_exists(conn, AUTO_DRAW_TABLE):
        return pd.DataFrame(columns=["round", "date", "number", "source"])
    df = pd.read_sql_query(
        f"SELECT draw_round AS round, draw_date AS date, number, source FROM {AUTO_DRAW_TABLE} WHERE game = ?",
        conn, params=(game,),
    )
    if df.empty:
        return pd.DataFrame(columns=["round", "date", "number", "source"])
    df["round"] = pd.to_numeric(df["round"], errors="coerce")
    df["date"] = df["date"].fillna("").apply(normalize_date)
    df["number"] = df["number"].apply(lambda x: normalize_number(x, digits))
    df["source"] = df["source"].fillna("auto_draw_results")
    df = df.dropna(subset=["round", "number"]).copy()
    df["round"] = df["round"].astype(int)
    return df[["round", "date", "number", "source"]]


def merge_draws(base: pd.DataFrame, auto: pd.DataFrame) -> pd.DataFrame:
    base = base.copy()
    base["source"] = "db"
    merged = pd.concat([base, auto], ignore_index=True)
    merged["_priority"] = merged["source"].apply(lambda x: 2 if x == "rakuten" else 1)
    return (merged.sort_values(["round", "_priority"])
            .drop_duplicates("round", keep="last")
            .drop(columns=["_priority"])
            .sort_values("round")
            .reset_index(drop=True))


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


_CANDIDATE_CACHE = {}

def _candidate_universe(digits: int):
    cached = _CANDIDATE_CACHE.get(digits)
    if cached is not None:
        return cached

    n = 10 ** digits
    nums = np.arange(n, dtype=np.int32)
    divs = (10 ** np.arange(digits - 1, -1, -1)).astype(np.int32)
    digit_matrix = ((nums[:, None] // divs[None, :]) % 10).astype(np.int8)
    strings = np.array([str(i).zfill(digits) for i in range(n)], dtype=object)
    digit_sums = digit_matrix.sum(axis=1).astype(np.float64)

    repeat_bonus = np.zeros(n, dtype=np.float64)
    for i in range(n):
        counts = np.bincount(digit_matrix[i], minlength=10)
        vals = counts[counts > 0]
        if digits == 3:
            if np.any(vals == 2):
                repeat_bonus[i] += 4.5
            if np.any(vals == 3):
                repeat_bonus[i] += 1.2
        else:
            if np.any(vals == 2):
                repeat_bonus[i] += 4.0
            if np.any(vals == 3):
                repeat_bonus[i] += 2.2
            if np.any(vals == 4):
                repeat_bonus[i] -= 1.0

    cached = (strings, digit_matrix, digit_sums, repeat_bonus)
    _CANDIDATE_CACHE[digits] = cached
    return cached


def generate_v7_predictions(history_numbers: list[str], digits: int, ranks: list[int]) -> list[str]:
    return generate_hybrid_predictions(history_numbers, digits, ranks)

# =====================================================
# 判定
# =====================================================
def judge_hit_type(pred_list: list[str], actual: str) -> tuple[str, str]:
    hit = str(actual)
    if any(str(pred) == hit for pred in pred_list):
        return "straight", "◎"
    if any(sorted(str(pred)) == sorted(hit) for pred in pred_list):
        return "box", "〇"
    return "none", "×"

# =====================================================
# シミュレーション
# =====================================================
def simulate(
    draws: pd.DataFrame,
    digits: int,
    ranks: list[int],
    start_after: int,
    last_n: int | None,
) -> pd.DataFrame:
    rows = []

    if len(draws) <= start_after:
        raise RuntimeError(f"データ数が不足しています。rows={len(draws)}, start_after={start_after}")

    start_idx = start_after
    end_idx = len(draws)

    if last_n is not None and last_n > 0:
        start_idx = max(start_idx, end_idx - last_n)

    for idx in range(start_idx, end_idx):
        target = draws.iloc[idx]
        train = draws.iloc[:idx]

        history_numbers = train["number"].dropna().astype(str).tolist()
        actual = str(target["number"])
        preds = generate_v7_predictions(history_numbers, digits, ranks)

        hit_type, mark = judge_hit_type(preds, actual)

        rows.append({
            "draw": int(target["round"]),
            "date": str(target["date"]),
            "actual": actual,
            "predictions": " / ".join(preds),
            "mainline": " / ".join(preds[:2]),
            "support": " / ".join(preds[2:]),
            "hit_type": hit_type,
            "mark": mark,
        })

        done = idx - start_idx + 1
        total = end_idx - start_idx

        if done % 50 == 0 or done == total:
            print(f"  progress {done}/{total} ...")

    return pd.DataFrame(rows)


def print_summary(name: str, df: pd.DataFrame) -> None:
    total = len(df)
    straight = int((df["hit_type"] == "straight").sum())
    box_total = int(df["hit_type"].isin(["straight", "box"]).sum())
    print(f"\n=== {name} summary ===")
    print(f"rows          = {total}")
    print(f"straight_hits = {straight}")
    print(f"box_hits      = {box_total}")
    if total:
        print(f"straight_rate = {straight / total:.4%}")
        print(f"box_rate      = {box_total / total:.4%}")

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="numbers.db")
    parser.add_argument("--out-n3", default="sim_numbers3.csv")
    parser.add_argument("--out-n4", default="sim_numbers4.csv")
    parser.add_argument("--start-after", type=int, default=500, help="最低学習件数。500推奨。")
    parser.add_argument("--last-n", type=int, default=1000, help="直近何回を検証するか。0なら全件。")
    args = parser.parse_args()

    db_path = Path(args.db_path)

    if not db_path.exists():
        raise FileNotFoundError(f"numbers.db が見つかりません: {db_path.resolve()}")

    last_n = args.last_n if args.last_n and args.last_n > 0 else None

    print("=== build_sim_numbers_v7 ===")
    print(f"db_path     = {db_path}")
    print(f"start_after = {args.start_after}")
    print(f"last_n      = {last_n if last_n is not None else 'ALL'}")

    conn = connect_db(str(db_path))

    print("\n=== load numbers3 ===")
    base_n3 = load_draws(conn, TABLE_N3, 3)
    draws_n3 = merge_draws(base_n3, load_auto_draws(conn, "N3", 3))
    print(f"N3 rows = {len(draws_n3)}")
    print(f"N3 round range = {draws_n3['round'].min()} - {draws_n3['round'].max()}")

    print("\n=== simulate numbers3 ===")
    sim_n3 = simulate(
        draws=draws_n3,
        digits=3,
        ranks=RANK_N3,
        start_after=args.start_after,
        last_n=last_n,
    )

    print("\n=== load numbers4 ===")
    base_n4 = load_draws(conn, TABLE_N4, 4)
    draws_n4 = merge_draws(base_n4, load_auto_draws(conn, "N4", 4))
    print(f"N4 rows = {len(draws_n4)}")
    print(f"N4 round range = {draws_n4['round'].min()} - {draws_n4['round'].max()}")

    print("\n=== simulate numbers4 ===")
    sim_n4 = simulate(
        draws=draws_n4,
        digits=4,
        ranks=RANK_N4,
        start_after=args.start_after,
        last_n=last_n,
    )

    out_n3 = Path(args.out_n3)
    out_n4 = Path(args.out_n4)

    sim_n3.to_csv(out_n3, index=False, encoding="utf-8-sig")
    sim_n4.to_csv(out_n4, index=False, encoding="utf-8-sig")

    print_summary("numbers3", sim_n3)
    print_summary("numbers4", sim_n4)

    print("\n=== saved ===")
    print(f"N3: {out_n3.resolve()}")
    print(f"N4: {out_n4.resolve()}")


if __name__ == "__main__":
    main()
