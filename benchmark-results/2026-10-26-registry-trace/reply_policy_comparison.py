"""Check whether unrestricted ingress among broker peers avoids reply losses."""
import contextlib,json,pathlib,subprocess,sys,time
ROOT=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(ROOT.parent/'2026-10-26-saturation'))
import run
run.ROOT=ROOT
original=run.kjson(['get','networkpolicy','mochi-cluster','-o','json'])
(ROOT/'network-policy-original.json').write_text(json.dumps(original,indent=2)+'\n')
rules=original['spec']['ingress']
peer={'from':[{'podSelector':original['spec']['podSelector']}]}
report=[];logs=[];procs=[]
def patch(ingress,name):
    path=ROOT/f'network-policy-{name}-patch.json'
    path.write_text(json.dumps({'spec':{'ingress':ingress}}))
    subprocess.run(run.K+['patch','networkpolicy','mochi-cluster','--type=merge','--patch-file',str(path)],check=True,stdout=subprocess.DEVNULL)
    time.sleep(3)
def snapshot():
    return {n:subprocess.check_output(['docker','exec',n,'sh','-c','conntrack -S; cat /proc/net/netfilter/nfnetlink_queue'],text=True) for n in ['desktop-worker2','desktop-worker3','desktop-worker']}
try:
    for i in range(3):
        f=(ROOT/f'mochi-cluster-{i}-reply-policy-debug.log').open('w');logs.append(f)
        procs.append(subprocess.Popen(run.K+['logs','-f',f'mochi-cluster-{i}','--since=2s'],stdout=f,stderr=subprocess.STDOUT))
    patch(rules+[peer],'allow-peer-replies')
    for attempt in range(3):
        before=snapshot();label=f'policy-allow-peer-replies-{attempt}'
        with (ROOT/'policy-run-output.jsonl').open('a') as out,contextlib.redirect_stdout(out):
            result=run.run(['-label',label,'-connections','6000','-concurrency','256','-subscribe'])
        report.append({'label':label,'before':before,'after':snapshot(),'benchmark':result.get('benchmark'),'broker_deltas':result['broker_deltas']})
        (ROOT/'reply-policy-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'label':label,'benchmark':result.get('benchmark')}),flush=True)
finally:
    patch(rules,'restore')
    for p in procs:p.terminate();p.wait(timeout=5)
    for f in logs:f.close()
