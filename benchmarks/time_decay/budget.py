"""Budget tradeoffs on indexed inputs, with an independent numerical oracle."""
from decimal import Decimal, localcontext
import math

SETTING='exponential_time_decay_aggregate_function_calculation_budget'


def run_budget(g):
    if g['A'].mode!='fork-on':
        raise ValueError('Budget suite requires experimental aggregates')
    budgets=list(map(float,g['A'].budgets.split(',')))
    if 0 not in budgets or any(not math.isfinite(b) or b<0 for b in budgets):
        raise ValueError('Include exact budget zero and only finite non-negative budgets')
    n=max(map(int,g['A'].sizes.split(',')))
    groups=max(1,n//1000); n=groups*1000
    sql,rows,save,measure,check=g['sql'],g['rows'],g['save'],g['measure'],g['check']
    reports=[]
    for dataset in ['positive','signed']:
        tab=g['DB']+'.budget_input'
        g['SETTINGS'][SETTING]=0
        sql(f'''CREATE TABLE {tab} (id UInt64,seq UInt64,v Float64,t Float64,
          d ExponentialTimeDecayingFloat64(600)) ENGINE=MergeTree ORDER BY (seq,id)''')
        sign='1.' if dataset=='positive' else 'if(seq%3=0,-1.,1.)'
        v=f'({sign})*(1+(id*17+seq*13)%1024)/16.'
        sql(f'''INSERT INTO {tab} SELECT id,seq,v,t,{g['decay']('v','t')} FROM
          (SELECT number%{groups} id,intDiv(number,{groups}) seq,{v} v,toFloat64(seq*60) t FROM numbers({n}))''')
        # Binary-exact input coefficients and integer timestamps avoid an SQL-exp oracle.
        with localcontext() as ctx:
            ctx.prec=80
            decay=[((Decimal(seq*60)-60000)/600).exp() for seq in range(1000)]
            oracle={};scales={}
            for key in range(groups):
                terms=[Decimal(1+(key*17+seq*13)%1024)/16*decay[seq]*
                       (-1 if dataset=='signed' and seq%3==0 else 1) for seq in range(1000)]
                oracle[key]=sum(terms,Decimal(0));scales[key]=sum(map(abs,terms),Decimal(0))
            save(f'budget_{dataset}_oracle.json',dict(rows=n,groups=groups,decay_length=600,target_time=60000,
                generator='id=number%groups; seq=number//groups; v=sign*(1+(id*17+seq*13)%1024)/16; t=seq*60',
                signed=dataset=='signed',expected={str(k):str(v) for k,v in oracle.items()}))
            sources={
              'indexed_forward':f'(SELECT * FROM {tab} ORDER BY seq,id LIMIT {n})',
              'indexed_reverse':f'(SELECT * FROM {tab} ORDER BY seq DESC,id LIMIT {n})',
              'raw_control':tab}
            queries={path:f'''SELECT id,exponentialTimeDecayingValueAt(
              {"exponentialTimeDecayedSum(600)(v,t)" if path=="raw_control" else "exponentialTimeDecayedSum(d)"},60000) value
              FROM {source} GROUP BY id ORDER BY id''' for path,source in sources.items()}
            queries['indexed_state_merge']=f'''SELECT id,exponentialTimeDecayingValueAt(exponentialTimeDecayedSumMerge(s),60000) value
              FROM (SELECT id,seq%8 batch,exponentialTimeDecayedSumState(d) s FROM {tab} GROUP BY id,batch)
              GROUP BY id ORDER BY id'''
            control=None
            # Fixed shuffled order avoids always measuring larger budgets last; zero first for controls.
            order=[0.]+[b for b in [8.,1.,16.,4.] if b in budgets]+[b for b in budgets if b not in [0.,8.,1.,16.,4.]]
            for budget in order:
                g['SETTINGS'][SETTING]=budget
                for path,q in queries.items():
                    label=f'budget_{dataset}_{budget:g}_{path}'
                    actual=rows(q); details=[]
                    for r in actual:
                        key=int(r['id']);value=float(r['value']);ref=oracle[key]
                        err=abs(Decimal.from_float(value)-ref)
                        details.append(dict(id=key,actual=value,expected=str(ref),absolute_error=float(err),
                            relative_error=float(err/abs(ref)) if ref else None,
                            l1_normalized_error=float(err/scales[key]),
                            sign_correct=(value>0)-(value<0)==(ref>0)-(ref<0)))
                    truth=sorted(oracle,key=lambda k:(-oracle[k],k))
                    ranked=[int(r['id']) for r in sorted(actual,key=lambda r:(-r['value'],int(r['id'])))]
                    topk=min(100,max(1,groups//10))
                    record=dict(name=label,dataset=dataset,path=path,calculation_budget=budget,rows=n,groups=groups,
                        max_absolute_error=max(d['absolute_error'] for d in details),
                        max_relative_error=max(d['relative_error'] for d in details if d['relative_error'] is not None),
                        max_l1_normalized_error=max(d['l1_normalized_error'] for d in details),
                        sign_errors=sum(not d['sign_correct'] for d in details),
                        positional_accuracy=sum(a==b for a,b in zip(ranked,truth))/len(truth),
                        top_k=topk,top_k_recall=len(set(ranked[:topk])&set(truth[:topk]))/topk,details=details)
                    reports.append(record);save('budget_accuracy.json',reports)
                    # Nonzero budgets intentionally approximate; report error instead of applying the exact gate.
                    good=len(actual)==groups and all(math.isfinite(d['actual']) for d in details)
                    if budget==0 or path=='raw_control':good=good and record['max_l1_normalized_error']<1e-10
                    check(label+'_valid',[good],[True])
                    if path=='raw_control':
                        if budget==0:control=actual
                        check(label+'_unchanged',actual,control)
                    measure(label,q,True)
        g['SETTINGS'][SETTING]=0
        sql(f'DROP TABLE {tab}')
