"""Observe conntrack state on registry replies without changing verdicts."""
import json,pathlib,subprocess,time
from live_validation import ROOT,K,NS,Admin,kjson,workload
nodes=['desktop-worker','desktop-worker2','desktop-worker3']
procs=[];gzips=[];files=[];installed=[]
def capture(cmd,path):
    f=path.open('wb');files.append(f)
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL);procs.append(p)
    gzips.append(subprocess.Popen(['gzip','-c'],stdin=p.stdout,stdout=f));p.stdout.close()
with Admin() as admin:
    stats=admin.stats();leader=next(v['Leader'] for v in stats.values() if v['Leader'])
    pods=kjson(['-n',NS,'get','pods','-o','json'])['items'];leader_ip=next(p['status']['podIP'] for p in pods if p['metadata']['name']==leader)
    prefix=f'ip saddr {leader_ip} tcp sport 7947'
    rules='table inet mochi_registry_trace {\n chain replies {\n type filter hook postrouting priority 94; policy accept;\n'
    rules+=f' {prefix} meta nftrace set 1\n'
    for state in ['invalid','new','established','related','untracked']:
        rules+=f' {prefix} ct state {state} counter comment "registry_state_{state}"\n'
    rules+=f' {prefix} ct direction reply counter comment "registry_direction_reply"\n'
    rules+=f' {prefix} ct direction original counter comment "registry_direction_original"\n'
    rules+=f' {prefix} ct label 28 counter comment "registry_label_present"\n }}\n}}\n'
    (ROOT/'reply-state-observer.nft').write_text(rules)
    try:
        for node in nodes:
            subprocess.run(['docker','exec','-i',node,'nft','-f','-'],input=rules,text=True,check=True);installed.append(node)
            capture(['docker','exec',node,'nft','monitor','trace'],ROOT/f'reply-state-{node}.log.gz')
        for p in pods:capture(K+['-n',NS,'logs','-f',p['metadata']['name'],'--since=1s'],ROOT/f"{p['metadata']['name']}-reply-trace.log.gz")
        agents=kjson(['-n','kube-system','get','pods','-l','app=kindnet','-o','json'])['items']
        for p in agents:
            if p['spec']['nodeName'] in nodes:capture(K+['-n','kube-system','logs','-f',p['metadata']['name'],'--since=1s'],ROOT/f"kindnet-{p['spec']['nodeName']}-reply-trace.log.gz")
        time.sleep(.5)
        runs=[]
        for trial in range(2):
            r=workload(f'reply-state-{trial}',['-connections','6000','-concurrency','256','-subscribe']);runs.append(r)
            if r.get('benchmark',{}).get('failed',0)>0:break
        time.sleep(1)
        tables={node:subprocess.check_output(['docker','exec',node,'nft','list','table','inet','mochi_registry_trace'],text=True) for node in nodes}
        (ROOT/'reply-state-summary.json').write_text(json.dumps({'leader':leader,'leader_ip':leader_ip,'before':stats,'after':admin.stats(),'tables':tables,'runs':runs},indent=2)+'\n')
    finally:
        for p in procs:p.terminate();p.wait(timeout=5)
        for p in gzips:p.wait(timeout=5)
        for f in files:f.close()
        for node in installed:subprocess.run(['docker','exec',node,'nft','delete','table','inet','mochi_registry_trace'],check=True)
