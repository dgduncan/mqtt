"""Measure original/raised/restored CPU limits without restarting Kindnet."""
import json,pathlib,subprocess,time
ROOT=pathlib.Path(__file__).resolve().parent
K=['kubectl','--kubeconfig=/Users/dduncan/.kube/config','--context=docker-desktop']
nodes=['desktop-worker','desktop-worker2','desktop-worker3']
def get(args):return json.loads(subprocess.check_output(K+args))
pods=get(['-n','kube-system','get','pods','-l','app=kindnet','-o','json'])['items']
pods=[p for p in pods if p['spec']['nodeName'] in nodes]
logs=[];files=[];gzips=[];report=[]
def snapshot():
    out={}
    for p in pods:
        node=p['spec']['nodeName']
        cid=subprocess.check_output(['docker','exec',node,'crictl','ps','--name','kindnet-cni','-q'],text=True).strip()
        item=json.loads(subprocess.check_output(['docker','exec',node,'crictl','inspect',cid]))
        pid=str(item['info']['pid'])
        cg=subprocess.check_output(['docker','exec',node,'cat',f'/proc/{pid}/cgroup'],text=True).strip().split(':',2)[2]
        base='/sys/fs/cgroup'+cg
        cpu=subprocess.check_output(['docker','exec',node,'cat',base+'/cpu.stat',base+'/cpu.max'],text=True)
        network=subprocess.check_output(['docker','exec',node,'sh','-c','conntrack -S; cat /proc/net/netfilter/nfnetlink_queue'],text=True)
        out[node]={'pid':pid,'cpu':cpu,'network':network}
    return out
def resize(cpu):
    for p in pods:
        name=p['metadata']['name']
        resources={'limits':{'cpu':cpu,'memory':'50Mi'},'requests':{'cpu':cpu,'memory':'50Mi'}}
        patch={'spec':{'containers':[{'name':'kindnet-cni','resources':resources}]}}
        subprocess.run(K+['-n','kube-system','patch','pod',name,'--subresource=resize','--type=strategic','-p',json.dumps(patch)],check=True,stdout=subprocess.DEVNULL)
    time.sleep(3)
try:
    for p in pods:
        name=p['metadata']['name'];node=p['spec']['nodeName']
        f=(ROOT/f'kindnet-{node}-cpu-diagnostic.log.gz').open('wb');files.append(f)
        log=subprocess.Popen(K+['-n','kube-system','logs','-f',name,'--since=1s'],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL);logs.append(log)
        gzips.append(subprocess.Popen(['gzip','-c'],stdin=log.stdout,stdout=f))
        log.stdout.close()
    for phase,cpu in [('original','100m'),('raised','1'),('restored','100m')]:
        if phase!='original':resize(cpu)
        for attempt in range(3):
            label=f'kindnet-cpu-{phase}-{attempt}';before=snapshot()
            cmd=K+['-n','mochi','exec','mochi-loadtest','--','timeout','40','/loadtest','-pub',','.join(f'mochi-cluster-{i}.mochi-cluster-peers:1883' for i in range(3)),'-connections','6000','-concurrency','256','-subscribe','-json','-label',label]
            result=subprocess.run(cmd,capture_output=True,text=True,timeout=45)
            try:b=json.loads(result.stdout.strip().splitlines()[-1])
            except (ValueError,IndexError):b=None
            item={'label':label,'cpu':cpu,'before':before,'after':snapshot(),'exit_code':result.returncode,'benchmark':b,'stderr':result.stderr}
            report.append(item);(ROOT/'kindnet-cpu-comparison.json').write_text(json.dumps(report,indent=2)+'\n')
            print(json.dumps({'label':label,'benchmark':b,'exit_code':result.returncode}),flush=True)
finally:
    resize('100m')
    for p in logs:p.terminate();p.wait(timeout=5)
    for p in gzips:p.wait(timeout=5)
    for f in files:f.close()
