"""Exercise a real mixed-version upgrade only in the disposable release."""
import json,subprocess,time
from live_validation import ROOT,K,NS,Admin,kjson,workload
H=['helm','--kubeconfig=/Users/dduncan/.kube/config','--kube-context=docker-desktop']
report=[]
for tag in ['auth-fix','reliability']:
    started=time.monotonic();entry={'target':tag,'timeline':[]};report.append(entry)
    with (ROOT/f'rolling-{tag}.log').open('w') as log:
        p=subprocess.Popen(H+['upgrade','mochi-validation','deploy/helm/mochi-cluster','-n',NS,'-f',str(ROOT/'validation-values.json'),'--set',f'image.tag={tag}','--wait','--timeout','150s'],stdout=log,stderr=log)
        while p.poll() is None:
            pods=kjson(['-n',NS,'get','pods','-o','json'])['items']
            entry['timeline'].append({'seconds':time.monotonic()-started,'pods':[{ 'name':v['metadata']['name'],'ip':v['status'].get('podIP'),'image':v['spec']['containers'][0]['image'],'ready':any(c['type']=='Ready' and c['status']=='True' for c in v['status'].get('conditions',[])),'deleting':bool(v['metadata'].get('deletionTimestamp'))} for v in pods]})
            (ROOT/'rolling-validation.json').write_text(json.dumps(report,indent=2)+'\n');time.sleep(1)
        entry['exit_code']=p.returncode
    entry['seconds']=time.monotonic()-started
    if p.returncode:
        entry['pods']=kjson(['-n',NS,'get','pods','-o','json'])
        (ROOT/'rolling-validation.json').write_text(json.dumps(report,indent=2)+'\n')
        raise SystemExit(f'rolling stage {tag} failed; inspect log')
    with Admin() as admin:
        entry['stats']=admin.stats();entry['health']={str(i):admin.get(i,'/healthz') for i in range(3)}
    entry['churn']=workload(f'rolling-{tag}-churn',['-connections','1000','-concurrency','64','-subscribe'])
    entry['qos2']=workload(f'rolling-{tag}-qos2',['-qos','2','-publishers','3','-subscribers','3','-messages','1000','-rate','1000','-timeout','10s'])
    (ROOT/'rolling-validation.json').write_text(json.dumps(report,indent=2)+'\n')
