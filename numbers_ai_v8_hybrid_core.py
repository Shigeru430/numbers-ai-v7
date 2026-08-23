from __future__ import annotations
from collections import Counter, defaultdict

PRODUCTION_LOGIC={"N3":"V8_REPEAT_PENALTY_BOX_CLASS_TOP5","N4":"V7_FIXED_RANK"}

def build_stats(history_numbers:list[str],digits:int)->dict:
    history_numbers=[str(n).zfill(digits) for n in history_numbers if str(n).isdigit() and len(str(n))<=digits]
    long_pos=[Counter() for _ in range(digits)]
    recent_pos=[Counter() for _ in range(digits)]
    total_digit=Counter()
    transition=[defaultdict(Counter) for _ in range(digits)]
    for n in history_numbers:
        for i,ch in enumerate(n):long_pos[i][ch]+=1;total_digit[ch]+=1
    for n in history_numbers[-80:]:
        for i,ch in enumerate(n):recent_pos[i][ch]+=1
    for a,b in zip(history_numbers[:-1],history_numbers[1:]):
        for i in range(digits):transition[i][a[i]][b[i]]+=1
    sums=[sum(map(int,n)) for n in history_numbers[-300:]]
    return {"digits":digits,"long_pos":long_pos,"recent_pos":recent_pos,"total_digit":total_digit,
            "transition":transition,"prev":history_numbers[-1] if history_numbers else None,
            "avg_sum":sum(sums)/len(sums) if sums else None,"max_total":max(total_digit.values()) if total_digit else 1}

def repeat_bonus_v7(c:str,d:int)->float:
    cnt=Counter(c);b=0.0
    if d==3:
        if 2 in cnt.values():b+=4.5
        if 3 in cnt.values():b+=1.2
    elif d==4:
        if 2 in cnt.values():b+=4.0
        if 3 in cnt.values():b+=2.2
        if 4 in cnt.values():b-=1.0
    return b

def v7_score(c:str,st:dict)->float:
    s=0.0
    for i,ch in enumerate(c):
        s+=st["long_pos"][i][ch]*0.45
        s+=st["recent_pos"][i][ch]*1.20
        s+=(st["max_total"]-st["total_digit"][ch])*0.18
        if st["prev"] is not None:s+=st["transition"][i][st["prev"][i]][ch]*1.15
    s+=repeat_bonus_v7(c,st["digits"])
    if st["avg_sum"] is not None:s-=abs(sum(map(int,c))-st["avg_sum"])*0.22
    return s

def _n3(history:list[str])->list[str]:
    st=build_stats(history,3);best={}
    for i in range(1000):
        c=str(i).zfill(3)
        s=v7_score(c,st)-1.50*repeat_bonus_v7(c,3)
        k="".join(sorted(c))
        if k not in best or s>best[k][0] or (s==best[k][0] and c<best[k][1]):best[k]=(s,c)
    ranked=sorted(((k,s,c) for k,(s,c) in best.items()),key=lambda x:(-x[1],x[0],x[2]))
    return [c for _,_,c in ranked[:5]]

def _n4(history:list[str],ranks:list[int])->list[str]:
    st=build_stats(history,4);sc=[]
    for i in range(10000):
        c=str(i).zfill(4);sc.append((c,v7_score(c,st)))
    sc.sort(key=lambda x:(-x[1],x[0]))
    return [sc[r-1][0] for r in ranks]

def generate_hybrid_predictions(history_numbers:list[str],digits:int,ranks:list[int])->list[str]:
    if digits==3:return _n3(history_numbers)
    if digits==4:return _n4(history_numbers,ranks)
    raise ValueError(digits)
