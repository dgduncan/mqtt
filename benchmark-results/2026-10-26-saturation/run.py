"""Run one in-cluster loadtest and retain its result plus broker telemetry."""
import concurrent.futures,json,os,pathlib,select,socket,subprocess,sys,threading,time,urllib.request
ROOT=pathlib.Path(__file__).resolve().parent
K=['kubectl','--kubeconfig=/Users/dduncan/.kube/config','--context=docker-desktop','-n','mochi']
BROKERS=','.join(f'mochi-cluster-{i}.mochi-cluster-peers:1883' for i in range(3))
NODES=['desktop-control-plane','desktop-worker','desktop-worker2','desktop-worker3']

def kjson(args):return json.loads(subprocess.check_output(K+args,timeout=8))
def exact(s,n):
    out=b''
    while len(out)<n:
        x=s.recv(n-len(out))
        if not x:raise EOFError()
        out+=x
    return out

def packet(s):
    h=exact(s,1)[0]; n=0; shift=0
    while True:
        b=exact(s,1)[0]; n+=(b&127)<<shift; shift+=7
        if not b&128:break
    return h,exact(s,n)

def string(v):
    b=v.encode();return len(b).to_bytes(2,'big')+b

class SysMonitor:
    def __init__(self,i):
        self.values={}; self.stop=False
        for attempt in range(6):
            self.s=socket.create_connection(('127.0.0.1',39883+i),timeout=3)
            b=string('MQTT')+b'\x04\x02\x00\x3c'+string('benchmark-sys-'+str(i)+'-'+str(time.time_ns()))
            try:
                self.s.sendall(b'\x10'+bytes([len(b)])+b);packet(self.s);break
            except (OSError,EOFError):
                self.s.close()
                if attempt==5:raise
                time.sleep(.2)
        b=b'\x00\x01'+string('$SYS/broker/#')+b'\x00'
        self.s.sendall(b'\x82'+bytes([len(b)])+b)
        self.thread=threading.Thread(target=self.loop,daemon=True);self.thread.start()
    def loop(self):
        ping=time.monotonic()
        try:
            while not self.stop:
                if time.monotonic()-ping>20:self.s.sendall(b'\xc0\x00');ping=time.monotonic()
                if not select.select([self.s],[],[],.5)[0]:continue
                h,b=packet(self.s)
                if h>>4==3:
                    n=int.from_bytes(b[:2],'big');topic=b[2:2+n].decode();v=b[2+n:].decode()
                    try:v=int(v)
                    except ValueError:pass
                    self.values[topic.removeprefix('$SYS/broker/')]=v
        except (OSError,EOFError):pass
    def close(self):self.stop=True;self.s.close();self.thread.join(timeout=2)

def resources():
    def node(n):
        try:
            data=kjson(['get','--raw',f'/api/v1/nodes/{n}/proxy/stats/summary'])
            return {p['podRef']['name']:{'cpu_cores':p.get('cpu',{}).get('usageNanoCores',0)/1e9,'working_set_mib':p.get('memory',{}).get('workingSetBytes',0)/1048576} for p in data['pods'] if p['podRef']['namespace']=='mochi'}
        except Exception as e:return {'error-'+n:str(e)}
    out={}
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        for d in pool.map(node,NODES):out.update(d)
    return out

def run(args):
    label=args[args.index('-label')+1] if '-label' in args else str(time.time_ns())
    pfs=[];monitors=[];samples=[];stop=threading.Event()
    try:
        for i in range(3):
            pfs.append(subprocess.Popen(K+['port-forward',f'pod/mochi-cluster-{i}',f'{39080+i}:8080',f'{39883+i}:1883'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL))
            for retry in range(100):
                try:
                    with socket.create_connection(('127.0.0.1',39080+i),timeout=.2):pass
                    break
                except OSError:time.sleep(.1)
            else:raise RuntimeError('port-forward failed')
            monitors.append(SysMonitor(i))
        time.sleep(.25)
        def snapshot(with_resources=False):
            nodes={}
            for i in range(3):
                try:
                    stats=json.load(urllib.request.urlopen(f'http://127.0.0.1:{39080+i}/stats',timeout=2))
                    nodes[f'mochi-cluster-{i}']={'stats':stats,'sys':monitors[i].values.copy()}
                except Exception as e:nodes[f'mochi-cluster-{i}']={'error':str(e)}
            out={'time':time.time(),'label':label,'nodes':nodes}
            if with_resources:out['resources']=resources()
            return out
        before=snapshot(True)
        def watch():
            while not stop.is_set():
                samples.append(snapshot(True));stop.wait(3)
        thread=threading.Thread(target=watch,daemon=True);thread.start()
        cmd=K+['exec','mochi-loadtest','--','timeout','110','/loadtest','-pub',BROKERS,'-sub',BROKERS,'-json']+args
        started=time.time()
        proc=subprocess.run(cmd,capture_output=True,text=True,timeout=120)
        time.sleep(1.25)
        after=snapshot(True);stop.set();thread.join(timeout=12)
        result={'label':label,'exit_code':proc.returncode,'wall_seconds':time.time()-started,'command':cmd,'stdout':proc.stdout,'stderr':proc.stderr,'before':before,'after':after}
        lines=proc.stdout.splitlines()
        for line in reversed(lines):
            try:result['benchmark']=json.loads(line);break
            except ValueError:pass
        deltas={}
        for name,node in after['nodes'].items():
            prev=before['nodes'].get(name,{})
            sd={f:node.get('stats',{}).get(f,0)-prev.get('stats',{}).get(f,0) for f in ['Dropped','Waits','LinkResyncs','ClaimFailures','ConnectionsRefused','ClientsFenced']}
            sd['subscriber_messages_dropped']=node.get('sys',{}).get('messages/dropped',0)-prev.get('sys',{}).get('messages/dropped',0)
            deltas[name]=sd
        result['broker_deltas']=deltas
        peak={}
        for snap in [before]+samples+[after]:
            for pod,r in snap.get('resources',{}).items():
                if not isinstance(r,dict):continue
                target=peak.setdefault(pod,{'cpu_cores':0,'working_set_mib':0})
                for f in target:target[f]=max(target[f],r.get(f,0))
        result['sampled_resource_peaks']=peak
        pods=kjson(['get','pods','-o','json'])
        result['pod_status']=[{'name':p['metadata']['name'],'phase':p['status']['phase'],'containers':[{'ready':c.get('ready'),'restarts':c.get('restartCount'),'lastState':c.get('lastState')} for c in p['status'].get('containerStatuses',[])]} for p in pods['items']]
        with (ROOT/'results.jsonl').open('a') as f:f.write(json.dumps(result)+'\n')
        with (ROOT/'telemetry.jsonl').open('a') as f:
            for snap in samples:f.write(json.dumps(snap)+'\n')
        summary={k:v for k,v in result.items() if k in ['label','exit_code','benchmark','broker_deltas','sampled_resource_peaks']}
        print(json.dumps(summary),flush=True)
        if 'benchmark' not in result:print(proc.stdout+proc.stderr,flush=True)
        return result
    finally:
        stop.set()
        for m in monitors:m.close()
        for p in pfs:p.terminate()
        for p in pfs:
            try:p.wait(timeout=3)
            except subprocess.TimeoutExpired:p.kill();p.wait()

if __name__=='__main__':run(sys.argv[1:])
