import datetime,json,pathlib
from analyze_packets import ROOT,errors,packets
failures=errors('boundary-debug')
wanted={(r['src'],r['sport'],r['dst'],r['dport']) for r in failures}
flows={}
for node in (0,2):
    for location in ('node','pod'):
        name=f'{location}-{node}'
        for p in packets(ROOT/f'boundary-{name}.pcap'):
            key=(p['src'],p['sport'],p['dst'],p['dport']);reverse=(p['dst'],p['dport'],p['src'],p['sport'])
            if key not in wanted and reverse not in wanted:continue
            flow=key if key in wanted else reverse
            flows.setdefault(flow,{}).setdefault(name,[]).append(p)
report=[]
for r in failures:
    key=(r['src'],r['sport'],r['dst'],r['dport']);captures=flows.get(key,{})
    if not captures:continue
    # The same TCP tuple can be reused in this churn workload. Select the last
    # connection instance before the timeout, rather than mixing older replies.
    deadline=datetime.datetime.fromisoformat(r['log_time'][:26]+'+00:00').timestamp()
    syns=[p for ps in captures.values() for p in ps if p['src']==r['src'] and p['flags']==2 and p['time']<=deadline]
    if syns:
        latest=max(syns,key=lambda p:p['time'])
        start=min(p['time'] for p in syns if p['seq']==latest['seq'])
        captures={name:[p for p in ps if p['time']>=start] for name,ps in captures.items()}
    base=min(p['time'] for ps in captures.values() for p in ps)
    report.append(dict(r,captures={name:[dict(p,offset_ms=round((p['time']-base)*1000,3)) for p in ps] for name,ps in captures.items()}))
(ROOT/'failed-rpc-boundary.json').write_text(json.dumps(report,indent=2)+'\n')
print('failed calls',len(failures),'captured',len(report))
for r in report[:3]:
    print('FLOW',r['sport'],r['log_time'])
    for name,ps in r['captures'].items():
        print(name,[(p['offset_ms'],p['src'],hex(p['flags']),p['seq'],p['ack'],p['payload_bytes']) for p in ps])
