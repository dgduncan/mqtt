"""Resize only the existing NFQUEUE receive sockets; restore them after A/B/A."""
import json,subprocess
from live_validation import ROOT,K,NS,workload
original=json.loads((ROOT/'kindnet-socket-original.json').read_text())
def configure(size=None):
    out={}
    for node,item in original.items():
        pid=item['pid'];args=['docker','exec',node,'/usr/local/bin/mochi-netlink-buffer',pid]
        if size is not None:args.append(str(size if size else json.loads(item['stdout'])['before']//2))
        p=subprocess.run(args,capture_output=True,text=True,check=True)
        out[node]={'socket':json.loads(p.stdout),'netlink':subprocess.check_output(['docker','exec',node,'cat',f'/proc/{pid}/net/netlink'],text=True),'conntrack':subprocess.check_output(['docker','exec',node,'conntrack','-S'],text=True)}
    return out
report=[]
try:
    for phase in ['original','enlarged','restored']:
        if phase=='enlarged':configure(4*1024*1024)
        if phase=='restored':configure(0)
        for attempt in range(3):
            before=configure();label=f'nfqueue-buffer-{phase}-{attempt}'
            r=workload(label,['-connections','6000','-concurrency','256','-subscribe'])
            report.append({'phase':phase,'before':before,'after':configure(),'result':r})
            (ROOT/'socket-buffer-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
finally:configure(0)
