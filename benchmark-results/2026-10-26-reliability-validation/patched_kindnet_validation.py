"""Churn and QoS validation after the persistent Kindnet buffer patch."""
import json,subprocess,time
from live_validation import ROOT,K,NS,Admin,kjson,workload

def snapshot():
    pods=kjson(['-n','kube-system','get','pods','-l','app=kindnet','-o','json'])['items'];out={}
    for p in pods:
        node=p['spec']['nodeName'];name=p['metadata']['name'];cid=p['status']['containerStatuses'][0]['containerID'].split('://')[1]
        pid=str(json.loads(subprocess.check_output(['docker','exec',node,'crictl','inspect',cid]))['info']['pid'])
        deadline=time.monotonic()+30
        while True:
            probe=subprocess.run(['docker','exec',node,'/usr/local/bin/mochi-netlink-buffer',pid],capture_output=True,text=True)
            if probe.returncode==0:break
            if time.monotonic()>=deadline:raise RuntimeError(probe.stderr)
            time.sleep(.2)
        out[node]={'pod':name,'pid':pid,'socket':json.loads(probe.stdout),'logs':subprocess.check_output(K+['-n','kube-system','logs',name],text=True),'conntrack':subprocess.check_output(['docker','exec',node,'conntrack','-S'],text=True)}
    return out

if __name__=='__main__':
    report={'before':snapshot(),'churn':[]}
    with Admin() as admin:
        report['stats_before']=admin.stats()
        for concurrency in [256,256,256,512,512,512]:
            report['churn'].append(workload(f'kindnet-patched-{concurrency}-{len(report["churn"])}',['-connections','6000','-concurrency',str(concurrency),'-subscribe']))
            (ROOT/'kindnet-patched-validation.json').write_text(json.dumps(report,indent=2)+'\n')
        report['qos2']=workload('kindnet-patched-qos2',['-qos','2','-publishers','3','-subscribers','3','-messages','1000','-rate','1000','-timeout','10s'])
        report['stats_after']=admin.stats();report['health_after']={str(i):admin.get(i,'/healthz') for i in range(3)}
    report['after']=snapshot()
    (ROOT/'kindnet-patched-validation.json').write_text(json.dumps(report,indent=2)+'\n')
