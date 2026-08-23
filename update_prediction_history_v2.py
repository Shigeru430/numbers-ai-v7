from numbers_ai_v8_hybrid_core import generate_hybrid_predictions
# Numbers AI v8 Hybrid Production Overlay v2
# update_prediction_history_v2.py
# Numbers AI v8 operation: update prediction_history_v7 without opening Streamlit.

import argparse, json, re, sqlite3
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
import pandas as pd

TABLE_N3='numbers3_enriched'; TABLE_N4='numbers4_enriched'
HISTORY_TABLE='prediction_history_v7'; AUTO_DRAW_TABLE='auto_draw_results'
RANK_N3=[4,5,9,11,20]; RANK_N4=[3,6,7,9,17]

def connect_db(p):
    c=sqlite3.connect(p); c.row_factory=sqlite3.Row; return c

def table_exists(conn,t):
    return conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?",(t,)).fetchone() is not None

def ensure_tables(conn):
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {HISTORY_TABLE}(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game TEXT NOT NULL,
        target_round INTEGER NOT NULL,
        target_date TEXT,
        pred_json TEXT NOT NULL,
        actual_number TEXT,
        eval_mark TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        UNIQUE(game,target_round))""")
    conn.execute(f"""CREATE TABLE IF NOT EXISTS {AUTO_DRAW_TABLE}(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        game TEXT NOT NULL,
        draw_round INTEGER NOT NULL,
        draw_date TEXT,
        number TEXT NOT NULL,
        source TEXT,
        fetched_at TEXT NOT NULL,
        UNIQUE(game,draw_round))""")
    conn.commit()

def find_col(cols, aliases):
    norm={str(c).lower():c for c in cols}
    for a in aliases:
        if a.lower() in norm: return norm[a.lower()]
    for c in cols:
        low=str(c).lower()
        for a in aliases:
            if a.lower() in low: return c
    return None

def normalize_number(v,digits):
    if v is None or isinstance(v,bool): return None
    if isinstance(v,int):
        s=str(v)
    elif isinstance(v,float):
        if not math.isfinite(v) or not v.is_integer(): return None
        s=str(int(v))
    else:
        s=str(v).strip()
        if s in ['', 'nan', 'None', 'NaN', '---', '-']: return None
        m=re.fullmatch(r'([+-]?\d+)\.0+',s)
        if m: s=m.group(1)
        if not re.fullmatch(r'[+-]?\d+',s): return None
    s=s.lstrip('+')
    if s.startswith('-') or len(s)>digits: return None
    return s.zfill(digits)

def load_draws(conn, table, digits):
    if not table_exists(conn,table): raise RuntimeError(f'table not found: {table}')
    df=pd.read_sql_query(f'SELECT * FROM {table}',conn)
    if df.empty: raise RuntimeError(f'table empty: {table}')
    cols=list(df.columns)
    rcol=find_col(cols,['round','draw_round','draw_no','draw_number','kaigou','回号','抽選回','抽せん回','no','times'])
    dcol=find_col(cols,['date','draw_date','抽選日','抽せん日','ymd','日付'])
    ncol=find_col(cols,['winning_number','winning','number','numbers','result','当選番号','当せん番号','本数字','num','番号'])
    digit_cols=[find_col(cols,[f'digit_{i}',f'digit{i}',f'd{i}',f'num{i}',f'n{i}']) for i in range(1,digits+1)]
    if rcol is None: df['_round_auto']=range(1,len(df)+1); rcol='_round_auto'
    if dcol is None: df['_date_auto']=''; dcol='_date_auto'
    if ncol is not None:
        df['_number']=df[ncol].apply(lambda x: normalize_number(x,digits))
    elif all(c is not None for c in digit_cols):
        df['_number']=df[digit_cols].astype(str).agg(''.join,axis=1).apply(lambda x: normalize_number(x,digits))
    else:
        raise RuntimeError(f'cannot detect number column: {table} columns={cols}')
    out=pd.DataFrame({'round':pd.to_numeric(df[rcol],errors='coerce'),'date':df[dcol].fillna('').astype(str),'number':df['_number']})
    out=out.dropna(subset=['round','number']).copy()
    out['round']=out['round'].astype(int); out['number']=out['number'].astype(str)
    out=out[out['number'].str.fullmatch(r'\d{'+str(digits)+r'}',na=False)].copy()
    if out.empty: raise RuntimeError(f'no valid {digits}-digit rows: {table}')
    return out.sort_values('round').drop_duplicates('round',keep='last').reset_index(drop=True)

def load_auto_draws(conn,game,digits):
    if not table_exists(conn,AUTO_DRAW_TABLE): return pd.DataFrame(columns=['round','date','number','source'])
    df=pd.read_sql_query(f"SELECT draw_round AS round, draw_date AS date, number, source FROM {AUTO_DRAW_TABLE} WHERE game=?",conn,params=(game,))
    if df.empty: return pd.DataFrame(columns=['round','date','number','source'])
    df['round']=pd.to_numeric(df['round'],errors='coerce'); df['date']=df['date'].fillna('').astype(str)
    df['number']=df['number'].apply(lambda x: normalize_number(x,digits)); df['source']=df['source'].fillna('auto_draw_results')
    df=df.dropna(subset=['round','number']).copy(); df['round']=df['round'].astype(int)
    df=df[df['number'].str.fullmatch(r'\d{'+str(digits)+r'}',na=False)].copy()
    return df[['round','date','number','source']]

def merge_draws(base,auto):
    base=base.copy(); base['source']=base.get('source','db')
    m=pd.concat([base,auto],ignore_index=True)
    if m.empty: return m
    m['_priority']=m['source'].apply(lambda x: 2 if x=='rakuten' else 1)
    m=m.sort_values(['round','_priority']).drop_duplicates('round',keep='last').drop(columns=['_priority'])
    return m.sort_values('round').reset_index(drop=True)

def build_stats(hist,digits):
    long=[Counter() for _ in range(digits)]; recent=[Counter() for _ in range(digits)]
    total=Counter(); trans=[defaultdict(Counter) for _ in range(digits)]
    prev=hist[-1] if hist else None
    for num in hist:
        for i,ch in enumerate(num): long[i][ch]+=1; total[ch]+=1
    for num in hist[-80:]:
        for i,ch in enumerate(num): recent[i][ch]+=1
    for a,b in zip(hist[:-1],hist[1:]):
        for i in range(digits): trans[i][a[i]][b[i]]+=1
    sums=[sum(int(x) for x in n) for n in hist[-300:] if isinstance(n,str) and re.fullmatch(r'\d+',n)]
    return {'digits':digits,'long':long,'recent':recent,'total':total,'trans':trans,'prev':prev,'avg_sum':sum(sums)/len(sums) if sums else None,'max_total':max(total.values()) if total else 1}

def candidate_score(c,st):
    score=0.0; digits=st['digits']
    for i,ch in enumerate(c):
        score+=st['long'][i][ch]*0.45
        score+=st['recent'][i][ch]*1.20
        score+=(st['max_total']-st['total'][ch])*0.18
        if st['prev'] is not None: score+=st['trans'][i][st['prev'][i]][ch]*1.15
    cnt=Counter(c)
    if digits==3:
        if 2 in cnt.values(): score+=4.5
        if 3 in cnt.values(): score+=1.2
    if digits==4:
        if 2 in cnt.values(): score+=4.0
        if 3 in cnt.values(): score+=2.2
        if 4 in cnt.values(): score-=1.0
    if st['avg_sum'] is not None: score-=abs(sum(int(x) for x in c)-st['avg_sum'])*0.22
    return score

def generate_predictions(history_numbers: list[str], digits: int, ranks: list[int]) -> list[str]:
    return generate_hybrid_predictions(history_numbers, digits, ranks)

def judge(preds,actual):
    if actual in [None,'','---','-']:
        return '-'

    hit=str(actual)

    for p in preds:
        if str(p)==hit:
            return '◎'

    for p in preds:
        if sorted(str(p))==sorted(hit):
            return '〇'

    return '×'

def saved_prediction(conn,game,target_round):
    row=conn.execute(f"SELECT pred_json FROM {HISTORY_TABLE} WHERE game=? AND target_round=?",(game,target_round)).fetchone()
    if not row: return None
    try:
        p=json.loads(row['pred_json']); return [str(x) for x in p] if isinstance(p,list) else None
    except Exception: return None

def insert_prediction(conn,game,target_round,target_date,preds):
    now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cur=conn.execute(f"""INSERT OR IGNORE INTO {HISTORY_TABLE}
    (game,target_round,target_date,pred_json,actual_number,eval_mark,created_at,updated_at)
    VALUES (?,?,?,?,NULL,NULL,?,?)""",(game,target_round,target_date,json.dumps(preds,ensure_ascii=False),now,now))
    conn.commit(); return cur.rowcount>0

def get_or_create(conn,game,target_round,target_date,hist,digits,ranks):
    saved=saved_prediction(conn,game,target_round)
    if saved is not None: return saved,False
    preds=generate_predictions(hist,digits,ranks)
    return preds,insert_prediction(conn,game,target_round,target_date,preds)

def update_actuals(conn,game,draws):
    rows=conn.execute(f"SELECT id,target_round,pred_json FROM {HISTORY_TABLE} WHERE game=?",(game,)).fetchall()
    draw_map={int(r['round']):r['number'] for _,r in draws.iterrows()}; now=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    updated=0
    for row in rows:
        tr=int(row['target_round'])
        if tr not in draw_map: continue
        actual=draw_map[tr]; preds=json.loads(row['pred_json']); mark=judge(preds,actual)
        cur=conn.execute(f"""UPDATE {HISTORY_TABLE} SET actual_number=?, eval_mark=?, updated_at=?
        WHERE id=? AND (actual_number IS NULL OR actual_number!=? OR eval_mark IS NULL OR eval_mark!=?)""",(actual,mark,now,row['id'],actual,mark))
        updated+=cur.rowcount
    conn.commit(); return updated

def summary(conn):
    total=conn.execute(f"SELECT COUNT(*) c FROM {HISTORY_TABLE}").fetchone()['c']
    ev=conn.execute(f"SELECT COUNT(*) c FROM {HISTORY_TABLE} WHERE actual_number IS NOT NULL AND eval_mark IS NOT NULL").fetchone()['c']
    latest=conn.execute(f"SELECT target_round,game,actual_number,eval_mark,updated_at FROM {HISTORY_TABLE} ORDER BY target_round DESC, game ASC LIMIT 8").fetchall()
    return total,ev,[dict(x) for x in latest]

def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--db-path',default='numbers.db'); args=ap.parse_args()
    db=Path(args.db_path)
    if not db.exists(): raise FileNotFoundError(f'numbers.db not found: {db.resolve()}')
    print('=== Numbers AI v8 update_prediction_history ==='); print(f'db_path = {db}')
    conn=connect_db(str(db)); ensure_tables(conn)
    n3=merge_draws(load_draws(conn,TABLE_N3,3),load_auto_draws(conn,'N3',3))
    n4=merge_draws(load_draws(conn,TABLE_N4,4),load_auto_draws(conn,'N4',4))
    latest_n3=int(n3['round'].max()); latest_n4=int(n4['round'].max())
    target=max(latest_n3,latest_n4)+1; target_date=datetime.now().strftime('%Y-%m-%d')
    print(f'N3 latest round = {latest_n3}'); print(f'N4 latest round = {latest_n4}'); print(f'target round    = {target}')
    print('\n=== update actuals ===')
    print(f"N3 actuals updated = {update_actuals(conn,'N3',n3)}")
    print(f"N4 actuals updated = {update_actuals(conn,'N4',n4)}")
    print('\n=== create target predictions ===')
    p3,c3=get_or_create(conn,'N3',target,target_date,n3['number'].dropna().astype(str).tolist(),3,RANK_N3)
    p4,c4=get_or_create(conn,'N4',target,target_date,n4['number'].dropna().astype(str).tolist(),4,RANK_N4)
    print(f"N3 created = {c3} preds = {' / '.join(p3)}")
    print(f"N4 created = {c4} preds = {' / '.join(p4)}")
    total,ev,latest=summary(conn)
    print('\n=== history summary ==='); print(f'history rows   = {total}'); print(f'evaluated rows = {ev}')
    print('\n=== latest history ===')
    for r in latest: print(f"{r['game']} 第{r['target_round']}回 actual={r['actual_number']} eval={r['eval_mark']} updated={r['updated_at']}")
    print('\n=== done ===')

if __name__=='__main__': main()
