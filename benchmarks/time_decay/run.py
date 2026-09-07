#!/usr/bin/env python3
"""End-to-end time-decay benchmarks. Standard library only; an isolated server is required."""
import argparse
import hashlib
import json
import math
import os
import pathlib
import random
import statistics
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

P = argparse.ArgumentParser()
P.add_argument('--url', default='http://127.0.0.1:18123')
P.add_argument('--sizes', default='1000,10000,100000,1000000')
P.add_argument('--histories', default='1000000')
P.add_argument('--keys', type=int, default=10000)
P.add_argument('--sources', type=int, default=10000)
P.add_argument('--targets', type=int, default=1000)
P.add_argument('--repeats', type=int, default=3)
P.add_argument('--out', default='build/results')
P.add_argument('--mode', choices=['fork-on', 'fork-off', 'upstream'], default='fork-on')
P.add_argument('--suite', choices=['full', 'comparison', 'budget'], default='full')
P.add_argument('--budgets', default='0,1,4,8,16')
A = P.parse_args()
OUT = pathlib.Path(A.out)
OUT.mkdir(parents=True, exist_ok=True)
DB = 'decay_bench_' + uuid.uuid4().hex[:10]
RESULTS = []
CHECKS = []
ERRORS = []
SETTINGS = dict(allow_experimental_time_decay_aggregate_functions=1,
                exponential_time_decay_aggregate_function_calculation_budget=0,
                max_threads=2, max_memory_usage=4000000000, max_execution_time=180,
                use_query_cache=0, use_query_condition_cache=0, log_queries=1)
if A.mode == 'upstream':
    del SETTINGS['allow_experimental_time_decay_aggregate_functions']
    del SETTINGS['exponential_time_decay_aggregate_function_calculation_budget']
elif A.mode == 'fork-off':
    SETTINGS['allow_experimental_time_decay_aggregate_functions'] = 0


def sql(query, qid=None):
    params = dict(SETTINGS)
    if qid:
        params['query_id'] = qid
    req = urllib.request.Request(A.url + '/?' + urllib.parse.urlencode(params), data=query.encode())
    try:
        with urllib.request.urlopen(req, timeout=200) as r:
            return r.read().decode()
    except urllib.error.HTTPError as e:
        raise RuntimeError(e.read().decode()) from e


def save(name, data):
    (OUT / name).write_text(json.dumps(data, indent=2) + '\n')


def check(name, actual, expected, approximate=False):
    good = actual == expected
    if approximate:
        good = len(actual) == len(expected) and all(
            x[0] == y[0] and math.isclose(x[1], y[1], rel_tol=1e-10, abs_tol=1e-10)
            for x, y in zip(actual, expected))
    differences = [dict(position=i, actual=x, expected=y) for i, (x, y) in enumerate(zip(actual, expected)) if x != y]
    CHECKS.append(dict(name=name, passed=good, actual=actual[:10], expected=expected[:10],
        calculation_budget=SETTINGS.get('exponential_time_decay_aggregate_function_calculation_budget'),
        positional_accuracy=sum(x == y for x, y in zip(actual, expected))/max(1,len(expected)),
        recall=len(set(map(str,actual)) & set(map(str,expected)))/max(1,len(set(map(str,expected)))),
        actual_count=len(actual), expected_count=len(expected), first_differences=differences[:20],
        actual_sha256=hashlib.sha256(json.dumps(actual).encode()).hexdigest(),
        expected_sha256=hashlib.sha256(json.dumps(expected).encode()).hexdigest()))
    save('correctness.json', CHECKS)
    if not good:
        raise AssertionError(name)


def rows(query):
    return [json.loads(x) for x in sql(query + ' FORMAT JSONEachRow').splitlines()]


def ids(query):
    return [str(r['id']) for r in rows(query)]


def measure(name, query, explain=False):
    (OUT / (name + '.sql')).write_text(query + '\n')
    if explain:
        (OUT / (name + '.explain.txt')).write_text(sql('EXPLAIN indexes=1, projections=1 ' + query))
    mutating = not query.lstrip().upper().startswith('SELECT')
    timed_query = query if mutating else query + ' FORMAT Null'
    if not mutating:
        sql(timed_query)  # untimed warm-up; results checked separately
    for rep in range(1 if mutating else A.repeats):
        qid = DB + '_' + uuid.uuid4().hex
        start = time.perf_counter()
        sql(timed_query, qid)
        RESULTS.append(dict(name=name, repeat=rep, query_id=qid, wall_seconds=time.perf_counter()-start,
            calculation_budget=SETTINGS.get('exponential_time_decay_aggregate_function_calculation_budget')))
        save('measurements.json', RESULTS)


def compare_queries(name, queries, explain=False):
    # Shuffle case order deterministically to reduce systematic warm-cache/order bias.
    baseline = ids(next(iter(queries.values())))
    names = list(queries)
    random.Random(729).shuffle(names)
    for label in names:
        query = queries[label]
        check(name+'_'+label, ids(query), baseline)
        measure(name+'_'+label, query, explain)


def decay(v, t):
    return f"initializeAggregation('exponentialTimeDecayingFloat64(600)', toFloat64({v}), toFloat64({t}))"


def capability(name, action):
    try:
        action()
        return True
    except RuntimeError as e:
        ERRORS.append(dict(case=name, kind='unsupported_or_failed', error=str(e)))
        save('errors.json', ERRORS)
        print(name, 'FAILED:', str(e)[:180], flush=True)
        return False


def primitives(n):
    table = DB+'.primitive'
    sql(f'CREATE TABLE {table} (id UInt64, v Float64, t Float64, d ExponentialTimeDecayingFloat64(600)) ENGINE=MergeTree ORDER BY id')
    sql(f'''INSERT INTO {table} SELECT id,v,t,{decay('v','t')} FROM
        (SELECT number id, if(number%13=0,0., if(number%3=0,-1.,1.)*(1+cityHash64(number)%100000)/1000.) v,
        toFloat64(cityHash64(number+7)%1000) t FROM numbers({n}))''')
    # Complete order check (including negative and zero values); fixed tie breaker.
    queries = dict(native=f'SELECT id FROM {table} ORDER BY d DESC,id',
                   evaluated=f'SELECT id FROM {table} ORDER BY exponentialTimeDecayingValueAt(d,1000) DESC,id',
                   formula=f'SELECT id FROM {table} ORDER BY v*exp((t-1000)/600) DESC,id')
    compare_queries(f'primitive_{n}_sort', queries)
    for k in [1,10,100,1000]:
        compare_queries(f'primitive_{n}_top{k}', {x:q+f' LIMIT {k}' for x,q in queries.items()})
    threshold = decay('2.', '500.')
    comp = dict(native=f'SELECT id FROM {table} WHERE d>{threshold} ORDER BY id',
                formula=f'SELECT id FROM {table} WHERE v*exp((t-1000)/600)>2*exp((500.-1000)/600) ORDER BY id')
    compare_queries(f'primitive_{n}_compare', comp)
    # Actual aggregate state creation and merging, alongside explicit arithmetic.
    measure(f'primitive_{n}_state_create', f'SELECT id%100 bucket, exponentialTimeDecayedSumState(600)(v,t) FROM {table} GROUP BY bucket')
    stateq = f'''SELECT bucket id, exponentialTimeDecayingValueAt(exponentialTimeDecayedSumMerge(600)(s),1000) value
        FROM (SELECT id%100 bucket,id%7 batch,exponentialTimeDecayedSumState(600)(v,t) s FROM {table} GROUP BY bucket,batch)
        GROUP BY bucket ORDER BY id'''
    expected = rows(f'SELECT bucket id,value FROM (SELECT id%100 bucket, sum(v*exp((t-1000)/600)) value FROM {table} GROUP BY bucket) ORDER BY id')
    actual = rows(stateq)
    check(f'primitive_{n}_state_merge', [(x['id'],x['value']) for x in actual], [(x['id'],x['value']) for x in expected], True)
    measure(f'primitive_{n}_state_merge', stateq)
    sql(f'DROP TABLE {table}')


def storage(f):
    # Same per-key curve, divided across f separately inserted physical parts.
    raw=DB+'.raw'
    sql(f'CREATE TABLE {raw} (model UInt64,source UInt64,id UInt64,v Float64,t Float64) ENGINE=MergeTree ORDER BY (model,source,id)')
    tables={}
    for label in ['plain','minmax','projection','both']:
        tab=DB+'.'+label
        sql(f'''CREATE TABLE {tab} (model UInt64,source UInt64,id UInt64,
          d SimpleAggregateFunction(exponentialTimeDecayedSum,ExponentialTimeDecayingFloat64(600)))
          ENGINE=AggregatingMergeTree ORDER BY (model,source,id)''')
        sql(f"ALTER TABLE {tab} MODIFY SETTING deduplicate_merge_projection_mode='rebuild', number_of_free_entries_in_pool_to_execute_mutation=2, number_of_free_entries_in_pool_to_execute_optimize_entire_partition=2")
        sql(f'SYSTEM STOP MERGES {tab}')
        tables[label]=tab
        if label in ('minmax','both'):
            if not capability(label+'_index',lambda:sql(f'ALTER TABLE {tab} ADD INDEX decay_mm d TYPE minmax GRANULARITY 1')):
                tables.pop(label); continue
        if label in ('projection','both'):
            if not capability(label+'_projection',lambda:sql(f'ALTER TABLE {tab} ADD PROJECTION decay_order (SELECT * ORDER BY (model,source,d,id))')):
                tables.pop(label)
    sql(f'SYSTEM STOP MERGES {raw}')
    for b in range(f):
        sql(f'''INSERT INTO {raw} SELECT number%2, intDiv(number,2)%10,number,
           (1+cityHash64(number)%1000000)/1000.*2*((number+{b})%{f}+1)/({f}*({f}+1)),toFloat64(cityHash64(number+7)%1000) FROM numbers({A.keys})''')
        for tab in tables.values():
            sql(f'''INSERT INTO {tab} SELECT number%2,intDiv(number,2)%10,number,
            {decay(f'(1+cityHash64(number)%1000000)/1000.*2*((number+{b})%{f}+1)/({f}*({f}+1))', 'toFloat64(cityHash64(number+7)%1000)')} FROM numbers({A.keys})''')
    save(f'fragmentation_{f}_parts.json', rows(f"SELECT table,count() parts,sum(rows) rows FROM system.parts WHERE database='{DB}' AND active GROUP BY table"))
    for phase in ['fragmented','optimized']:
        if phase=='optimized':
            for tab in tables.values():
                sql(f'SYSTEM START MERGES {tab}')
                measure(f'frag{f}_{tab.split(".")[-1]}_optimize', f'OPTIMIZE TABLE {tab} FINAL')
                sql(f'SYSTEM STOP MERGES {tab}')
        for k in [1,10,100,1000]:
            # Complete merged truth; querying partial rows without merging has different semantics.
            baseline=f'SELECT id FROM {raw} WHERE model=0 AND source=0 GROUP BY id ORDER BY sum(v*exp((t-1000)/600)) DESC,id LIMIT {k}'
            queries={'formula':baseline}
            for label,tab in tables.items():
                queries[label+'_final']=f'SELECT id FROM {tab} FINAL WHERE model=0 AND source=0 ORDER BY d DESC,id LIMIT {k}'
                queries[label+'_group']=f'SELECT id FROM {tab} WHERE model=0 AND source=0 GROUP BY id ORDER BY exponentialTimeDecayedSum(d) DESC,id LIMIT {k}'
            compare_queries(f'frag{f}_{phase}_top{k}', queries, True)
        for label,tab in tables.items():
            # Approximate candidate selection: merge ALL rows for surviving keys.
            candidate=f'SELECT id FROM {tab} WHERE model=0 AND source=0 AND id IN (SELECT id FROM {tab} WHERE model=0 AND source=0 ORDER BY d DESC,id LIMIT 128) GROUP BY id ORDER BY exponentialTimeDecayedSum(d) DESC,id LIMIT 100'
            exact=ids(f'SELECT id FROM {raw} WHERE model=0 AND source=0 GROUP BY id ORDER BY sum(v*exp((t-1000)/600)) DESC,id LIMIT 100')
            chosen=ids(candidate)
            save(f'frag{f}_{phase}_{label}_candidate_quality.json',dict(exact=chosen==exact,
                recall=len(set(chosen)&set(exact))/max(1,len(exact)), candidate_row_budget=128,
                reference_ids=exact,candidate_ids=chosen))
            measure(f'frag{f}_{phase}_{label}_approximate_candidates',candidate,True)
        for label,tab in tables.items():
            # Physical-row comparison tests index/projection suitability without claiming logical Top-K.
            compare_queries(f'frag{f}_{phase}_{label}_physical',dict(
                formula=f'SELECT id FROM {tab} WHERE model=0 AND source=0 ORDER BY exponentialTimeDecayingValueAt(d,1000) DESC,id LIMIT 100',
                native=f'SELECT id FROM {tab} WHERE model=0 AND source=0 ORDER BY d DESC,id LIMIT 100'),True)
            # Value predicate necessary to test minmax pruning; ORDER BY alone is insufficient.
            compare_queries(f'frag{f}_{phase}_{label}_threshold',dict(
                formula=f'SELECT id FROM {tab} WHERE exponentialTimeDecayingValueAt(d,1000)>10 ORDER BY id',
                native=f'SELECT id FROM {tab} WHERE d>{decay("10.","1000.")} ORDER BY id'),True)
    for tab in [raw]+[DB+'.'+x for x in ['plain','minmax','projection','both']]:
        sql(f'DROP TABLE {tab}')


def candidate_counterexample():
    tab=DB+'.candidate'
    sql(f'CREATE TABLE {tab} (id UInt64,d SimpleAggregateFunction(exponentialTimeDecayedSum,ExponentialTimeDecayingFloat64(600))) ENGINE=AggregatingMergeTree ORDER BY id')
    sql(f'SYSTEM STOP MERGES {tab}')
    for v in [6,6]:
        sql(f'INSERT INTO {tab} SELECT 1,{decay(str(v)+".","1000.")}')
    sql(f'INSERT INTO {tab} SELECT 2,{decay("10.","1000.")}')
    truth=ids(f'SELECT id FROM {tab} GROUP BY id ORDER BY exponentialTimeDecayedSum(d) DESC,id LIMIT 1')
    candidate=ids(f'SELECT id FROM (SELECT * FROM {tab} ORDER BY d DESC,id LIMIT 1) GROUP BY id ORDER BY exponentialTimeDecayedSum(d) DESC,id LIMIT 1')
    check('candidate_counterexample_complete',truth,['1'])
    check('candidate_counterexample_truncated',candidate,['2'])
    save('candidate_warning.json',dict(exact_top1=truth,candidate_top1=candidate,exact=False,reason='Two partial values of 6 outrank one value of 10 after merging.'))
    sql(f'DROP TABLE {tab}')


def history(n):
    tab=DB+'.history'
    raw=DB+'.history_raw'
    sql(f'CREATE TABLE {raw} (source UInt64,id UInt64,v Float64,t Float64) ENGINE=MergeTree ORDER BY (source,id)')
    sql(f'CREATE TABLE {tab} (source UInt64,id UInt64,d SimpleAggregateFunction(exponentialTimeDecayedSum,ExponentialTimeDecayingFloat64(600))) ENGINE=AggregatingMergeTree ORDER BY (source,id)')
    sql(f'SYSTEM STOP MERGES {tab}')
    sql(f'SYSTEM STOP MERGES {raw}')
    # Deterministic mixture of uniform relations and injected hot relations.
    for b in range(10):
        lo=n*b//10; count=n*(b+1)//10-lo
        data=f'''SELECT cityHash64(number)%{A.sources} source,
          if(number%5=0,cityHash64(number+1)%8,cityHash64(number+1)%{A.targets}) id,
          1. v,toFloat64(number)/{n}*1000 t FROM numbers({lo},{count})'''
        measure(f'history{n}_insert{b}',f'INSERT INTO {raw} {data}')
        measure(f'history{n}_aggregate_insert{b}',f'INSERT INTO {tab} SELECT source,id,exponentialTimeDecayingFloat64(600)(v,t) FROM ({data}) GROUP BY source,id')
        if b in (0,4,9):
            size=n*(b+1)//10
            for k in [64,128]:
                compare_queries(f'history{n}_at{size}_top{k}',dict(
                    formula=f'SELECT source*{A.targets}+target id FROM (SELECT source,id target,sum(v*exp((t-1000)/600)) score FROM {raw} GROUP BY source,target) ORDER BY score DESC,id LIMIT {k}',
                    native=f'SELECT source*{A.targets}+target id FROM (SELECT source,id target,exponentialTimeDecayedSum(d) score FROM {tab} GROUP BY source,target) ORDER BY score DESC,id LIMIT {k}'),True)
    hot_sources=[int(x['source']) for x in rows(f'SELECT source FROM {raw} GROUP BY source ORDER BY sum(v*exp((t-1000)/600)) DESC,source LIMIT 16')]
    selected=','.join(map(str,hot_sources))
    measure(f'history{n}_active_sources_formula',f'SELECT source FROM {raw} GROUP BY source ORDER BY sum(v*exp((t-1000)/600)) DESC,source LIMIT 16')
    measure(f'history{n}_active_sources_native',f'SELECT source FROM {tab} GROUP BY source ORDER BY exponentialTimeDecayedSum(d) DESC,source LIMIT 16')
    check(f'history{n}_active_sources', [int(x['source']) for x in rows(f'SELECT source FROM {tab} GROUP BY source ORDER BY exponentialTimeDecayedSum(d) DESC,source LIMIT 16')],hot_sources)
    for k in [64,128]:
        compare_queries(f'history{n}_per_source_top{k}',dict(
            formula=f'SELECT source*{A.targets}+target id FROM (SELECT source,id target,sum(v*exp((t-1000)/600)) score FROM {raw} WHERE source IN ({selected}) GROUP BY source,target) ORDER BY source,score DESC,id LIMIT {k} BY source',
            native=f'SELECT source*{A.targets}+target id FROM (SELECT source,id target,exponentialTimeDecayedSum(d) score FROM {tab} WHERE source IN ({selected}) GROUP BY source,target) ORDER BY source,score DESC,id LIMIT {k} BY source'),True)
    save(f'history{n}_parts.json',rows(f"SELECT table,count() parts,sum(rows) rows,sum(bytes_on_disk) bytes FROM system.parts WHERE database='{DB}' AND active GROUP BY table"))
    sql(f'DROP TABLE {tab}');sql(f'DROP TABLE {raw}')


def finish():
    sql('SYSTEM FLUSH LOGS')
    metrics=rows(f"""SELECT query_id,query_duration_ms,read_rows,read_bytes,memory_usage,
        ProfileEvents['UserTimeMicroseconds'] cpu_user_us,ProfileEvents['SystemTimeMicroseconds'] cpu_system_us,
        ProfileEvents['SelectedParts'] selected_parts,ProfileEvents['SelectedMarks'] selected_marks,
        ProfileEvents['MemoryAllocations'] allocations,projections
        FROM system.query_log WHERE type='QueryFinish' AND startsWith(query_id,'{DB}_')""")
    byid={r['query_id']:r for r in metrics}
    for r in RESULTS:
        r.update(byid.get(r['query_id'],{}))
        r['server_rows_per_second'] = r['read_rows']*1000/r['query_duration_ms'] if r.get('query_duration_ms') else None
        r['query_log_record_found'] = r['query_id'] in byid
    save('measurements.json',RESULTS)
    lines=['# Time-decay benchmark results','',f'Source: `{os.environ.get("SOURCE_SHA","unknown")}`',
           f'{len(RESULTS)} timed queries; {len(CHECKS)} correctness checks ({sum(not c['passed'] for c in CHECKS)} failed); {len(ERRORS)} failed/unsupported operations.',
           '', 'Candidate-first truncation is not exact on fragmented state (see candidate_warning.json).',
           'Timings are warm-cache single-runner measurements, not production capacity claims.',
           '', '| Case | Median seconds |', '|---|---:|']
    for name in dict.fromkeys(r['name'] for r in RESULTS):
        lines.append(f'| {name} | {statistics.median(r["wall_seconds"] for r in RESULTS if r["name"]==name):.6f} |')
    (OUT/'summary.md').write_text('\n'.join(lines)+'\n')


sql(f'CREATE DATABASE {DB}')
save('environment.json',dict(source_sha=os.environ.get('SOURCE_SHA'),database=DB,args=vars(A),settings=SETTINGS,
     version=sql('SELECT version()').strip(),build_options=rows('SELECT name,value FROM system.build_options'),
     cpu=pathlib.Path('/proc/cpuinfo').read_text(),memory=pathlib.Path('/proc/meminfo').read_text()))
try:
    from accuracy import run_accuracy, portable, indexed
    if A.suite == 'budget':
        from budget import run_budget
        run_budget(globals())
    elif A.suite == 'comparison':
        run_accuracy(globals())
        for n in map(int,A.sizes.split(',')):
            print('portable',n,flush=True);portable(globals(),n)
        print('indexed layouts',flush=True);indexed(globals(),max(map(int,A.sizes.split(','))))
    else:
        run_accuracy(globals())
        if A.mode != 'fork-on':
            raise ValueError('Full custom-type suite requires fork-on')
        candidate_counterexample()
        for n in map(int,A.sizes.split(',')):
            print('primitives',n,flush=True);primitives(n)
        for f in [1,2,4,16,64]:
            print('fragmentation',f,flush=True);storage(f)
        for n in map(int,A.histories.split(',')):
            print('history',n,flush=True);history(n)
finally:
    finish()
if ERRORS:
    raise RuntimeError(f'{len(ERRORS)} unsupported/failed cases; inspect errors.json')
