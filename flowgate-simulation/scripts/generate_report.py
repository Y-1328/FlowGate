#!/usr/bin/env python3
"""Build CSV summaries, PNG tracking charts, Markdown and HTML reports from real event CSVs."""
from __future__ import annotations
import argparse, csv, json, math, statistics
from collections import defaultdict
from pathlib import Path
from datetime import datetime, timezone
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

FIELDS = ['task_id','adapter','scenario','workload','arrival_nanos','start_nanos','finish_nanos','status','deadline_nanos']

def percentile(values, p):
    if not values: return None
    vals = sorted(values)
    k = (len(vals)-1) * p
    lo, hi = math.floor(k), math.ceil(k)
    if lo == hi: return vals[lo]
    return vals[lo] + (vals[hi]-vals[lo])*(k-lo)

def load_events(results_dir):
    all_events=[]
    for path in sorted(results_dir.glob('SIM-*_*.csv')):
        if path.name.endswith('_workload_trace.csv'): continue
        with path.open(newline='', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                for key in ('task_id','arrival_nanos','start_nanos','finish_nanos','deadline_nanos'):
                    row[key]=int(row[key])
                row['queue_wait_ms']=(row['start_nanos']-row['arrival_nanos'])/1e6 if row['start_nanos'] else None
                row['service_ms']=(row['finish_nanos']-row['start_nanos'])/1e6 if row['start_nanos'] and row['finish_nanos'] else None
                row['latency_ms']=(row['finish_nanos']-row['arrival_nanos'])/1e6 if row['finish_nanos'] else None
                all_events.append(row)
    return all_events

def summarize(events):
    groups=defaultdict(list)
    for e in events: groups[(e['scenario'],e['adapter'])].append(e)
    rows=[]
    for (scenario,adapter), items in sorted(groups.items()):
        completed=[e for e in items if e['status'].startswith('COMPLETED')]
        rejected=[e for e in items if e['status'].startswith('REJECTED')]
        failed=[e for e in items if e['status']=='FAILED']
        waits=[e['queue_wait_ms'] for e in completed if e['queue_wait_ms'] is not None]
        lat=[e['latency_ms'] for e in completed if e['latency_ms'] is not None]
        service=[e['service_ms'] for e in completed if e['service_ms'] is not None]
        arrivals=[e['arrival_nanos'] for e in items]
        finishes=[e['finish_nanos'] for e in completed]
        span_s=((max(finishes)-min(arrivals))/1e9) if finishes and arrivals and max(finishes)>min(arrivals) else None
        throughput=(len(completed)/span_s) if span_s and span_s>0 else 0.0
        rows.append(dict(scenario=scenario,adapter=adapter,submitted=len(items),completed=len(completed),rejected=len(rejected),failed=len(failed),
            rejection_rate_pct=100*len(rejected)/len(items) if items else 0,
            throughput_tasks_s=throughput,
            queue_wait_p50_ms=percentile(waits,.50),queue_wait_p95_ms=percentile(waits,.95),queue_wait_p99_ms=percentile(waits,.99),
            latency_p50_ms=percentile(lat,.50),latency_p95_ms=percentile(lat,.95),latency_p99_ms=percentile(lat,.99),
            service_p50_ms=percentile(service,.50),service_p95_ms=percentile(service,.95),
            deadline_misses=sum(1 for e in completed if e['status']=='COMPLETED_LATE')))
    return rows

def write_csv(path, rows):
    if not rows: return
    with path.open('w',newline='',encoding='utf-8') as f:
        writer=csv.DictWriter(f,fieldnames=list(rows[0].keys())); writer.writeheader(); writer.writerows(rows)

def save_charts(charts_dir, events, summaries, worker_limits):
    charts_dir.mkdir(parents=True,exist_ok=True)
    # Throughput by scenario
    if summaries:
        scenarios=sorted(set(r['scenario'] for r in summaries)); adapters=sorted(set(r['adapter'] for r in summaries))
        width=.8/max(1,len(adapters)); x=list(range(len(scenarios)))
        fig,ax=plt.subplots(figsize=(12,6))
        for j,a in enumerate(adapters):
            vals=[next((r['throughput_tasks_s'] for r in summaries if r['scenario']==s and r['adapter']==a),0) for s in scenarios]
            ax.bar([v+(j-(len(adapters)-1)/2)*width for v in x],vals,width=width,label=a)
        ax.set_xticks(x,scenarios,rotation=30,ha='right'); ax.set_ylabel('Completed tasks / second'); ax.set_title('Throughput by scenario'); ax.legend(); fig.tight_layout(); fig.savefig(charts_dir/'throughput_by_scenario.png',dpi=150); plt.close(fig)
        fig,ax=plt.subplots(figsize=(12,6))
        for a in adapters:
            subset=[r for r in summaries if r['adapter']==a]
            ax.plot([r['scenario'] for r in subset],[r['latency_p95_ms'] or 0 for r in subset],marker='o',label=a)
        ax.set_ylabel('P95 end-to-end latency (ms)'); ax.set_title('P95 latency tracking'); ax.tick_params(axis='x',rotation=30); ax.legend(); fig.tight_layout(); fig.savefig(charts_dir/'p95_latency_tracking.png',dpi=150); plt.close(fig)
        fig,ax=plt.subplots(figsize=(12,6))
        for a in adapters:
            subset=[r for r in summaries if r['adapter']==a]
            ax.plot([r['scenario'] for r in subset],[r['rejection_rate_pct'] for r in subset],marker='o',label=a)
        ax.set_ylabel('Rejected submissions (%)'); ax.set_title('Admission / rejection tracking'); ax.tick_params(axis='x',rotation=30); ax.legend(); fig.tight_layout(); fig.savefig(charts_dir/'rejection_rate_tracking.png',dpi=150); plt.close(fig)
    # Per-scenario operational tracking derived from recorded arrival/start/finish events.
    grouped=defaultdict(list)
    for e in events: grouped[(e['scenario'],e['adapter'])].append(e)
    for scenario in sorted(set(k[0] for k in grouped)):
        figq, axq = plt.subplots(figsize=(10,5))
        figu, axu = plt.subplots(figsize=(10,5))
        figa, axa = plt.subplots(figsize=(10,5))
        any_data=False
        for (sc, adapter), items in sorted(grouped.items()):
            if sc != scenario: continue
            accepted=[e for e in items if e['start_nanos'] > 0 and e['finish_nanos'] > 0]
            if not accepted: continue
            any_data=True
            origin=min(e['arrival_nanos'] for e in items)
            q_events=[]; u_events=[]; a_events=[]
            for e in items:
                t=(e['arrival_nanos']-origin)/1e6
                a_events.append((t,1,0))
                if e['start_nanos'] > 0:
                    st=(e['start_nanos']-origin)/1e6; fn=(e['finish_nanos']-origin)/1e6
                    q_events.extend([(t,1),(st,-1)])
                    u_events.extend([(st,1),(fn,-1)])
                    a_events.append((fn,0,1))
            def step(series):
                series.sort(key=lambda z:z[0]); xs=[0.0]; ys=[0]; value=0
                for t,delta in series:
                    value+=delta; xs.append(t); ys.append(value)
                return xs,ys
            qx,qy=step(q_events); ux,uy=step(u_events)
            axq.step(qx,qy,where='post',label=adapter)
            limit=max(1,worker_limits.get(scenario,1))
            axu.step(ux,[100*v/limit for v in uy],where='post',label=adapter)
            a_events.sort(key=lambda z:z[0]); xs=[0.0]; arrivals=[0]; completions=[0]; av=cv=0
            for t,da,dc in a_events:
                av+=da; cv+=dc; xs.append(t); arrivals.append(av); completions.append(cv)
            axa.step(xs,arrivals,where='post',label=f'{adapter} arrivals')
            axa.step(xs,completions,where='post',linestyle='--',label=f'{adapter} completions')
        if any_data:
            axq.set_title(f'{scenario}: estimated queue depth'); axq.set_xlabel('Elapsed time (ms)'); axq.set_ylabel('Accepted tasks waiting to start'); axq.legend(); figq.tight_layout(); figq.savefig(charts_dir/f'{scenario}_queue_depth.png',dpi=140)
            axu.set_title(f'{scenario}: worker utilization'); axu.set_xlabel('Elapsed time (ms)'); axu.set_ylabel('Active workers (%)'); axu.set_ylim(bottom=0); axu.legend(); figu.tight_layout(); figu.savefig(charts_dir/f'{scenario}_worker_utilization.png',dpi=140)
            axa.set_title(f'{scenario}: arrivals vs completions'); axa.set_xlabel('Elapsed time (ms)'); axa.set_ylabel('Cumulative tasks'); axa.legend(); figa.tight_layout(); figa.savefig(charts_dir/f'{scenario}_arrivals_completions.png',dpi=140)
        plt.close(figq); plt.close(figu); plt.close(figa)
    # Latency distribution across all completed tasks
    by_adapter=defaultdict(list)
    for e in events:
        if e['latency_ms'] is not None and e['status'].startswith('COMPLETED'): by_adapter[e['adapter']].append(e['latency_ms'])
    if by_adapter:
        fig,ax=plt.subplots(figsize=(10,6)); ax.boxplot([by_adapter[k] for k in sorted(by_adapter)],tick_labels=sorted(by_adapter),showfliers=False)
        ax.set_ylabel('End-to-end latency (ms)'); ax.set_title('Latency distribution by adapter'); fig.tight_layout(); fig.savefig(charts_dir/'latency_distribution.png',dpi=150); plt.close(fig)
    # Workload status totals
    status=defaultdict(lambda:defaultdict(int))
    for e in events: status[e['adapter']][e['status'].split('_')[0]]+=1
    if status:
        labels=sorted(status); keys=sorted({k for v in status.values() for k in v}); bottom=[0]*len(labels)
        fig,ax=plt.subplots(figsize=(9,5))
        for key in keys:
            vals=[status[a][key] for a in labels]; ax.bar(labels,vals,bottom=bottom,label=key); bottom=[bottom[i]+vals[i] for i in range(len(vals))]
        ax.set_ylabel('Task count'); ax.set_title('Task outcomes by adapter'); ax.legend(); fig.tight_layout(); fig.savefig(charts_dir/'task_outcomes.png',dpi=150); plt.close(fig)

def fmt(v): return 'N/A' if v is None else f'{v:.3f}'
def main():
    ap=argparse.ArgumentParser(); ap.add_argument('--input',default='../benchmark-results/latest'); ap.add_argument('--output',default='../benchmark-results/latest/report'); args=ap.parse_args()
    inp=Path(args.input).resolve(); out=Path(args.output).resolve(); out.mkdir(parents=True,exist_ok=True)
    events=load_events(inp); summaries=summarize(events)
    if events:
        with (out/'combined_events.csv').open('w',newline='',encoding='utf-8') as f:
            writer=csv.DictWriter(f,fieldnames=FIELDS+['queue_wait_ms','service_ms','latency_ms']); writer.writeheader(); writer.writerows(events)
    write_csv(out/'scenario_metrics.csv',summaries)
    (out/'metrics.json').write_text(json.dumps({'generatedAt':datetime.now(timezone.utc).isoformat(),'eventCount':len(events),'scenarios':summaries,
        'measurementNotes':['Latency is calculated from adapter-recorded arrival to task completion.','Throughput is completed tasks divided by observed run span.','CPU, memory, allocation and GC metrics are not captured by this initial runner and are explicitly not reported.','SIM-09 cancellation pressure is currently marked unsupported by adapters.']},indent=2),encoding='utf-8')
    charts=out/'charts'
    manifest_path=inp/'manifest.json'
    worker_limits={}
    if manifest_path.exists():
        manifest=json.loads(manifest_path.read_text(encoding='utf-8'))
        worker_limits={x['id']:x['workers'] for x in manifest.get('scenarios',[])}
    save_charts(charts,events,summaries,worker_limits)
    rows='\n'.join(f"| {r['scenario']} | {r['adapter']} | {r['completed']} | {r['rejected']} | {r['failed']} | {r['throughput_tasks_s']:.2f} | {fmt(r['latency_p95_ms'])} | {r['rejection_rate_pct']:.2f}% |" for r in summaries)
    md=f'''# FlowGate Competitive Benchmark Report\n\nGenerated: {datetime.now(timezone.utc).isoformat()}\n\n## Scope and limitations\n\nThis is the initial runnable benchmark harness. It currently executes the real FlowGate public API and a bounded JDK `ThreadPoolExecutor` baseline. The other competitor adapters (Resilience4j, Netflix concurrency-limits, Failsafe and Project Reactor) are not wired into this build yet; they must not be represented as measured. SIM-09 cancellation pressure is explicitly skipped pending a common cancellation contract.\n\nThe workloads use fixed scenario definitions and deterministic task-class/failure assignment. Arrival timestamps and JVM scheduling are real measurements, so repeated runs will vary. This is not a virtual-time simulator.\n\n## Results\n\n| Scenario | Adapter | Completed | Rejected | Failed | Throughput (tasks/s) | P95 latency (ms) | Rejection rate |\n|---|---|---:|---:|---:|---:|---:|---:|\n{rows}\n\n## Charts\n\n- `charts/throughput_by_scenario.png`\n- `charts/p95_latency_tracking.png`\n- `charts/rejection_rate_tracking.png`\n- `charts/latency_distribution.png`\n- `charts/task_outcomes.png`\n- Per-scenario queue-depth, worker-utilization and arrivals/completions tracking charts\n\n## Measurement exclusions\n\nCPU, heap, allocation rate, GC, thread count, fairness score, and recovery time are not yet instrumented in this first runnable increment. They are not fabricated or inferred. Add JFR/OS sampling and richer task-class accounting before using those metrics in a public competitive claim.\n'''
    (out/'report.md').write_text(md,encoding='utf-8')
    html='''<!doctype html><html><head><meta charset="utf-8"><title>FlowGate Benchmark Report</title><style>body{font:15px system-ui;max-width:1200px;margin:40px auto;padding:0 20px;color:#20252b}table{border-collapse:collapse;width:100%}td,th{border:1px solid #ddd;padding:8px;text-align:left}th{background:#f3f5f7}.charts{display:grid;grid-template-columns:1fr 1fr;gap:18px}.charts img{width:100%;border:1px solid #ddd}code{background:#f3f5f7;padding:2px 4px}</style></head><body><h1>FlowGate Competitive Benchmark Report</h1><p>Generated '''+datetime.now(timezone.utc).isoformat()+'''</p><p>Initial adapters: FlowGate and bounded JDK ThreadPoolExecutor. Other named libraries are not yet measured. See report.md for limitations.</p><h2>Scenario metrics</h2><table><tr><th>Scenario</th><th>Adapter</th><th>Completed</th><th>Rejected</th><th>Failed</th><th>Throughput/s</th><th>P95 latency ms</th><th>Rejected %</th></tr>'''+''.join(f"<tr><td>{r['scenario']}</td><td>{r['adapter']}</td><td>{r['completed']}</td><td>{r['rejected']}</td><td>{r['failed']}</td><td>{r['throughput_tasks_s']:.2f}</td><td>{fmt(r['latency_p95_ms'])}</td><td>{r['rejection_rate_pct']:.2f}</td></tr>" for r in summaries)+'''</table><h2>Tracking charts</h2><div class="charts">'''+''.join(f'<figure><img src="charts/{p.name}"><figcaption>{p.stem.replace("_"," ")}</figcaption></figure>' for p in sorted(charts.glob('*.png')))+'''</div><h2>Limitations</h2><p>CPU/memory/GC/fairness/recovery metrics are not instrumented yet. SIM-09 cancellation pressure is skipped. No unsupported values are fabricated.</p></body></html>'''
    (out/'report.html').write_text(html,encoding='utf-8')
    print(f'Report generated at {out}')
if __name__=='__main__': main()
