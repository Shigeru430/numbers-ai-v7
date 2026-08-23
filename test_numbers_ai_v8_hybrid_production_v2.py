import ast,json,sqlite3
from pathlib import Path
from numbers_ai_v8_hybrid_core import generate_hybrid_predictions,PRODUCTION_LOGIC
def norm(v,d):
    if v is None:return None
    try:return str(int(float(v))).zfill(d)
    except:return None
def hist(c,t,g,d):
    vals={}
    for r,n in c.execute(f"select draw_no,number from {t}").fetchall():
        x=norm(n,d)
        if x:vals[int(r)]=x
    try:a=c.execute("select draw_round,number from auto_draw_results where game=?",(g,)).fetchall()
    except:a=[]
    for r,n in a:
        x=norm(n,d)
        if x:vals[int(r)]=x
    return [vals[r] for r in sorted(vals)]
checks={"n3_contract":PRODUCTION_LOGIC["N3"]=="V8_REPEAT_PENALTY_BOX_CLASS_TOP5","n4_contract":PRODUCTION_LOGIC["N4"]=="V7_FIXED_RANK"}
expected={"app.py":"generate_v7_predictions_cached","update_prediction_history_v2.py":"generate_predictions","build_sim_numbers_v7.py":"generate_v7_predictions"}
for fn,func in expected.items():
    p=Path(fn);txt=p.read_text(encoding="utf-8") if p.exists() else ""
    checks[fn+"_exists"]=p.exists();checks[fn+"_import"]="from numbers_ai_v8_hybrid_core import generate_hybrid_predictions" in txt;checks[fn+"_func"]=f"def {func}(" in txt
    try:ast.parse(txt);checks[fn+"_syntax"]=True
    except:checks[fn+"_syntax"]=False
db=Path("numbers.db");checks["db"]=db.exists();pred={}
if db.exists():
    c=sqlite3.connect(db);h3=hist(c,"numbers3_enriched","N3",3);h4=hist(c,"numbers4_enriched","N4",4);c.close()
    p3=generate_hybrid_predictions(h3,3,[4,5,9,11,20]);p4=generate_hybrid_predictions(h4,4,[3,6,7,9,17]);pred={"N3":p3,"N4":p4}
    checks["n3_five"]=len(p3)==5;checks["n4_five"]=len(p4)==5;checks["n3_box_unique"]=len({"".join(sorted(x)) for x in p3})==5
failed=[k for k,v in checks.items() if not v]
print(json.dumps({"tool":"Numbers AI v8 Hybrid Production SelfTest v2","status":"PASS" if not failed else "FAIL","checks":checks,"sample_next_predictions":pred,"failed_checks":failed},ensure_ascii=False,indent=2))
raise SystemExit(1 if failed else 0)
