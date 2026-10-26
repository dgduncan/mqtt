import json,pathlib,subprocess,sys,time
root=pathlib.Path(__file__).resolve().parent
sys.path.insert(0,str(root.parent/'2026-10-26-saturation'))
import run
run.ROOT=root
info=json.loads((root/'tcp-before.json').read_text());procs=[];files=[]
try:
    for i in (0,2):
        item=info[f'mochi-cluster-{i}'];node=item['node'];pid=item['pid']
        for location,extra in [('node',[]),('pod',['nsenter','-t',pid,'-n'])]:
            f=(root/f'boundary-{location}-{i}.pcap').open('wb');files.append(f)
            procs.append(subprocess.Popen(['docker','exec',node]+extra+['/usr/local/bin/mochi-registry-sniff','60'],stdout=f,stderr=subprocess.PIPE))
    for i in range(3):
        f=(root/f'mochi-cluster-{i}-boundary-debug.log').open('w');files.append(f)
        procs.append(subprocess.Popen(run.K+['logs','-f',f'mochi-cluster-{i}','--since=2s'],stdout=f,stderr=subprocess.STDOUT))
    time.sleep(1)
    for index,p in enumerate(procs[:4]):
        if p.poll() is not None:raise RuntimeError(f'capture {index} exit={p.returncode}: '+p.stderr.read().decode())
    for attempt in range(2):
        r=run.run(['-label',f'boundary-trace-{attempt}','-connections','6000','-concurrency','256','-subscribe'])
        if r['benchmark']['failed']>0:break
finally:
    for i in (0,2):
        node=info[f'mochi-cluster-{i}']['node']
        subprocess.run(['docker','exec',node,'pkill','-TERM','-f','^/usr/local/bin/mochi-registry-sniff'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    for p in procs[:4]:p.wait(timeout=5)
    for p in procs[4:]:p.terminate();p.wait()
    for f in files:f.close()
    for i in (0,2):
        node=info[f'mochi-cluster-{i}']['node']
        data=subprocess.check_output(['docker','exec',node,'sh','-c','cat /proc/net/netstat /proc/net/snmp; ip -s link; ip route; ip rule; iptables-save -c; tc qdisc show'],text=True)
        (root/f'boundary-node-{i}-after.txt').write_text(data)
print('Worker and pod captures saved.')
