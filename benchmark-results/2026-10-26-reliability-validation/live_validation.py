"""Exercise the isolated release; preserve the original broker cluster."""
import json,pathlib,socket,subprocess,time,urllib.request,urllib.error
ROOT=pathlib.Path(__file__).resolve().parent
K=['kubectl','--kubeconfig=/Users/dduncan/.kube/config','--context=docker-desktop']
NS='mochi-validation'
BROKERS=','.join(f'mochi-validation-{i}.mochi-validation-peers.{NS}.svc:1883' for i in range(3))
def kjson(args):return json.loads(subprocess.check_output(K+args))
class Admin:
    def __enter__(self):
        self.procs=[]
        for i in range(3):
            port=40680+i
            p=subprocess.Popen(K+['-n',NS,'port-forward',f'pod/mochi-validation-{i}',f'{port}:8080'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);self.procs.append(p)
            for _ in range(100):
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.2):pass
                    break
                except OSError:time.sleep(.1)
            else:raise RuntimeError('port-forward failed')
        return self
    def get(self,i,path):
        started=time.monotonic()
        try:
            with urllib.request.urlopen(f'http://127.0.0.1:{40680+i}{path}',timeout=3) as r:
                return {'status':r.status,'body':r.read().decode(),'seconds':time.monotonic()-started}
        except urllib.error.HTTPError as e:return {'status':e.code,'body':e.read().decode(),'seconds':time.monotonic()-started}
    def stats(self):return {f'mochi-validation-{i}':json.loads(self.get(i,'/stats')['body']) for i in range(3)}
    def __exit__(self,*args):
        for p in self.procs:p.terminate()
        for p in self.procs:p.wait(timeout=5)
def workload(label,args):
    cmd=K+['-n','mochi','exec','mochi-loadtest','--','timeout','45','/loadtest','-pub',BROKERS,'-sub',BROKERS,'-json','-label',label]+args
    p=subprocess.run(cmd,capture_output=True,text=True,timeout=50)
    result={'label':label,'command':cmd,'exit_code':p.returncode,'stdout':p.stdout,'stderr':p.stderr}
    try:result['benchmark']=json.loads(p.stdout.strip().splitlines()[-1])
    except (IndexError,ValueError):pass
    print(json.dumps(result),flush=True)
    with (ROOT/'live-results.jsonl').open('a') as f:f.write(json.dumps(result)+'\n')
    return result
if __name__=='__main__':
    with Admin() as admin:
        before=admin.stats();health={str(i):admin.get(i,'/healthz') for i in range(3)}
        metrics={str(i):admin.get(i,'/metrics')['body'] for i in range(3)}
        workload('validation-qos2',['-qos','2','-publishers','3','-subscribers','3','-messages','1000','-rate','1000','-timeout','10s'])
        workload('validation-churn',['-connections','6000','-concurrency','256','-subscribe'])
        leader=next(v['Leader'] for v in before.values() if v['Leader'])
        follower=next(name for name in before if name!=leader);i=int(follower.rsplit('-',1)[1])
        policy={'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':'validation-block-registry','namespace':NS},'spec':{'podSelector':{'matchLabels':{'statefulset.kubernetes.io/pod-name':follower}},'policyTypes':['Egress'],'egress':[{'ports':[{'protocol':'TCP','port':1,'endPort':7946},{'protocol':'TCP','port':7948,'endPort':65535},{'protocol':'UDP'}]}]}}
        path=ROOT/'block-registry-policy.json';path.write_text(json.dumps(policy,indent=2)+'\n')
        blocked={}
        try:
            subprocess.run(K+['apply','-f',str(path)],check=True,stdout=subprocess.DEVNULL);time.sleep(3)
            blocked={'follower':follower,'health':admin.get(i,'/healthz'),'stats':admin.stats()}
        finally:
            subprocess.run(K+['-n',NS,'delete','networkpolicy','validation-block-registry','--ignore-not-found'],check=True,stdout=subprocess.DEVNULL)
        time.sleep(3)
        report={'before':before,'health_before':health,'metrics':metrics,'blocked':blocked,'health_restored':{str(i):admin.get(i,'/healthz') for i in range(3)},'after':admin.stats()}
        (ROOT/'live-validation.json').write_text(json.dumps(report,indent=2)+'\n')
        print(json.dumps({'blocked_follower':follower,'blocked_health':blocked.get('health'),'restored_health':report['health_restored']}),flush=True)
