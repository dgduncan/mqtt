"""Compare identical connection churn with the chart NetworkPolicy on/off/on."""
import contextlib,json,pathlib,subprocess,sys,time
ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'2026-10-26-saturation'))
import run
run.ROOT=ROOT
HELM=['helm','--kubeconfig=/Users/dduncan/.kube/config','--kube-context=docker-desktop','upgrade','mochi-cluster',str(ROOT.parent.parent/'deploy/helm/mochi-cluster'),'-n','mochi','--reuse-values','--wait','--timeout','2m']
NODES=['desktop-worker2','desktop-worker3','desktop-worker']
def snapshot():
    return {n:subprocess.check_output(['docker','exec',n,'sh','-c','conntrack -S; cat /proc/net/netfilter/nfnetlink_queue; cat /proc/sys/net/netfilter/nf_conntrack_count /proc/sys/net/netfilter/nf_conntrack_max; nft list table inet kindnet-network-policies'],text=True) for n in NODES}
report=[];logs=[];procs=[]
try:
    for i in range(3):
        f=(ROOT/f'mochi-cluster-{i}-policy-debug.log').open('w');logs.append(f)
        procs.append(subprocess.Popen(run.K+['logs','-f',f'mochi-cluster-{i}','--since=2s'],stdout=f,stderr=subprocess.STDOUT))
    for phase,enabled in [('on-before',True),('off',False),('on-after',True)]:
        if phase!='on-before':
            subprocess.run(HELM+['--set',f'networkPolicy.enabled={str(enabled).lower()}'],check=True,stdout=subprocess.DEVNULL)
            time.sleep(3)
        for attempt in range(3):
            before=snapshot();label=f'policy-{phase}-{attempt}'
            with (ROOT/'policy-run-output.jsonl').open('a') as out,contextlib.redirect_stdout(out):
                result=run.run(['-label',label,'-connections','6000','-concurrency','256','-subscribe'])
            item={'label':label,'network_policy_enabled':enabled,'before':before,'after':snapshot(),'benchmark':result.get('benchmark'),'broker_deltas':result['broker_deltas']}
            report.append(item)
            (ROOT/'policy-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps({'label':label,'benchmark':item['benchmark']}),flush=True)
finally:
    subprocess.run(HELM+['--set','networkPolicy.enabled=true'],check=True,stdout=subprocess.DEVNULL)
    for p in procs:p.terminate();p.wait(timeout=5)
    for f in logs:f.close()
