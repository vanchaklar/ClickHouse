"""Portable comparisons, high-precision numerical oracles, and selective layouts."""
from decimal import Decimal, localcontext
import math
import random


def window_query(table):
    return f'''SELECT bucket id, score*exp((t-1000)/600) value FROM (
      SELECT bucket,t,seq,exponentialTimeDecayedSum(600)(v,t) OVER
      (PARTITION BY bucket ORDER BY t,seq ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) score
      FROM {table}) ORDER BY bucket,t DESC,seq DESC LIMIT 1 BY bucket'''


def run_accuracy(g):
    sql,rows,save,check=g['sql'],g['rows'],g['save'],g['check']
    tab=g['DB']+'.accuracy'
    sql(f'CREATE TABLE {tab} (bucket UInt64,seq UInt64,v Float64,t Float64) ENGINE=Memory')
    rng=random.Random(110934)
    data=[]
    for bucket in range(100):
        for seq in range(32):
            v=rng.uniform(0.01,100) if bucket<50 else rng.uniform(-100,100)
            data.append((bucket,seq,v,float(rng.randrange(-1000,1001))))
    # Cancellation, exact zero, and large magnitude: report absolute AND L1-normalized error.
    data += [(100,0,1e16,1000.),(100,1,1.,1000.),(100,2,-1e16,1000.),
             (101,0,8.,1000.),(101,1,-8.,1000.),(102,0,1e100,0.),(102,1,1e100,1000.)]
    save('accuracy_inputs.json',data)
    sql(f'INSERT INTO {tab} FORMAT TabSeparated\n'+'\n'.join('\t'.join(map(str,r)) for r in data))
    with localcontext() as ctx:
        ctx.prec=80
        expected={}; scales={}
        for bucket,seq,v,t in data:
            term=Decimal.from_float(v)*((Decimal.from_float(t)-1000)/600).exp()
            expected[bucket]=expected.get(bucket,Decimal(0))+term
            scales[bucket]=scales.get(bucket,Decimal(0))+abs(term)
        queries={'formula':f'SELECT bucket id,sum(v*exp((t-1000)/600)) value FROM {tab} GROUP BY bucket ORDER BY id',
                 'window':window_query(tab)}
        if g['A'].mode=='fork-on':
            queries['aggregate']=f'SELECT bucket id,exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(600)(v,t),1000) value FROM {tab} GROUP BY bucket ORDER BY id'
            queries['state_merge']=f'''SELECT bucket id,exponentialTimeDecayingValueAt(exponentialTimeDecayedSumMerge(600)(s),1000) value
              FROM (SELECT bucket,seq%7 batch,exponentialTimeDecayedSumState(600)(v,t) s FROM {tab} GROUP BY bucket,batch) GROUP BY bucket ORDER BY id'''
        report={}
        for label,q in queries.items():
            actual=rows(q); details=[]
            for r in actual:
                key=int(r['id']);value=float(r['value']);ref=expected[key]
                err=abs(Decimal.from_float(value)-ref)
                details.append(dict(id=key,actual=value,expected=str(ref),absolute_error=float(err),
                    relative_error=float(err/abs(ref)) if ref else None,
                    l1_normalized_error=float(err/scales[key]) if scales[key] else float(err),
                    sign_correct=(value>0)-(value<0)==(ref>0)-(ref<0)))
            order=[int(r['id']) for r in sorted(actual,key=lambda r:(-r['value'],int(r['id'])))]
            truth=sorted(expected,key=lambda k:(-expected[k],k))
            report[label]=dict(samples=len(details),max_absolute_error=max(x['absolute_error'] for x in details),
                max_relative_error=max(x['relative_error'] for x in details if x['relative_error'] is not None),
                max_l1_normalized_error=max(x['l1_normalized_error'] for x in details),
                sign_errors=sum(not x['sign_correct'] for x in details),
                positional_accuracy=sum(x==y for x,y in zip(order,truth))/len(truth),
                top10_recall=len(set(order[:10])&set(truth[:10]))/10,details=details)
            save('accuracy.json',report)
            check('accuracy_'+label+'_finite_and_scaled',
                  [len(details)==len(expected) and all(math.isfinite(x['actual']) and x['l1_normalized_error']<1e-10 for x in details)],[True])
    if g['A'].mode=='fork-off':
        rejected=[]
        for label,q in [
            ('aggregate',f'SELECT exponentialTimeDecayedSum(600)(v,t) FROM {tab}'),
            ('type',f'CREATE TABLE {g["DB"]}.gated (d ExponentialTimeDecayingFloat64(600)) ENGINE=Memory')]:
            try:sql(q)
            except RuntimeError as e:
                # Preserve the actual error; accept only an explicit experimental-feature rejection.
                message=str(e)
                good=('allow_experimental_time_decay_aggregate_functions' in message or 'experimental' in message.lower())
                rejected.append(dict(case=label,rejected=good,error=message))
            else:rejected.append(dict(case=label,rejected=False,error=None))
        save('flag_off.json',rejected)
        check('experimental_paths_rejected',[r['rejected'] for r in rejected],[True,True])
    sql(f'DROP TABLE {tab}')


def portable(g,n):
    sql,measure=g['sql'],g['measure'];tab=g['DB']+'.portable'
    sql(f'CREATE TABLE {tab} (seq UInt64,bucket UInt64,v Float64,t Float64) ENGINE=MergeTree ORDER BY seq')
    sql(f'''INSERT INTO {tab} SELECT number,number%100,
      if(number%13=0,0.,if(number%3=0,-1.,1.)*(1+cityHash64(number)%100000)/1000.),
      toFloat64(cityHash64(number+7)%1000) FROM numbers({n})''')
    for k in [None,1,10,100,1000]:
        measure(f'portable_{n}_'+('sort' if k is None else f'top{k}'),
          f'SELECT seq FROM {tab} ORDER BY v*exp((t-1000)/600) DESC,seq'+('' if k is None else f' LIMIT {k}'))
    measure(f'portable_{n}_formula_sum',f'SELECT bucket,sum(v*exp((t-1000)/600)) FROM {tab} GROUP BY bucket')
    measure(f'portable_{n}_window_sum',window_query(tab))
    if g['A'].mode=='fork-on':
        measure(f'portable_{n}_aggregate_sum',f'SELECT bucket,exponentialTimeDecayingValueAt(exponentialTimeDecayedSum(600)(v,t),1000) FROM {tab} GROUP BY bucket')
    sql(f'DROP TABLE {tab}')


def indexed(g,n=1000000):
    sql,ids,check,measure,save=g['sql'],g['ids'],g['check'],g['measure'],g['save']
    families=['float']+(['native'] if g['A'].mode=='fork-on' else [])
    for family in families:
        dtype='Float64' if family=='float' else 'ExponentialTimeDecayingFloat64(600)'
        def encode(v):return f'toFloat64({v})' if family=='float' else g['decay'](v,'1000.')
        for layout in ['plain','minmax','projection','both']:
            tab=g['DB']+'.indexed_'+family+'_'+layout
            sql(f'CREATE TABLE {tab} (id UInt64,model UInt64,source UInt64,d {dtype}) ENGINE=MergeTree ORDER BY (model,source,id)')
            sql(f'SYSTEM STOP MERGES {tab}')
            if layout in ['minmax','both']:sql(f'ALTER TABLE {tab} ADD INDEX decay_mm d TYPE minmax GRANULARITY 1')
            if layout in ['projection','both']:sql(f'ALTER TABLE {tab} ADD PROJECTION decay_order (SELECT * ORDER BY (model,source,d,id))')
            # Reversed identities make base primary order unsuitable for highest-score Top-K.
            # Separate score bands make selective predicates useful for minmax pruning.
            for b in range(16):
                lo=n*b//16;count=n*(b+1)//16-lo
                sql(f'INSERT INTO {tab} SELECT {n}-number,0,0,{encode("number+1")} FROM numbers({lo},{count})')
            cases={
              'top100':f'SELECT id FROM {tab} WHERE model=0 AND source=0 ORDER BY d DESC,id LIMIT 100',
              'threshold':f'SELECT id FROM {tab} WHERE d>{encode(str(n-n//1000))} ORDER BY id'}
            for case,q in cases.items():
                name=f'indexed_{n}_{family}_{layout}_{case}'
                expected=[str(x) for x in range(1,(min(n,100) if case=='top100' else n//1000)+1)]
                check(name,ids(q),expected)
                measure(name,q,True)
            save(f'indexed_{family}_{layout}_parts.json',g['rows'](f"SELECT count() parts,sum(rows) rows FROM system.parts WHERE database='{g['DB']}' AND table='indexed_{family}_{layout}' AND active"))
            sql(f'DROP TABLE {tab}')
