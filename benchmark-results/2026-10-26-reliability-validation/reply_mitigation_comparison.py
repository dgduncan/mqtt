"""Test accepting only established replies between validation broker IPs."""
import json,os,pathlib,re,subprocess,time
from live_validation import ROOT,K,NS,Admin,kjson,workload
nodes=['desktop-worker','desktop-worker2','desktop-worker3']
COMMENT='mochi-validation-established-replies'
VARIANT=os.environ.get('MOCHI_COMPARISON_VARIANT','')
def remove():
    for node in nodes:
        chain=subprocess.check_output(['docker','exec',node,'nft','-a','list','chain','inet','kindnet-network-policies','postrouting'],text=True)
        for line in chain.splitlines():
            if COMMENT not in line:continue
            handle=re.search(r'# handle (\d+)',line)[1]
            subprocess.run(['docker','exec',node,'nft','delete','rule','inet','kindnet-network-policies','postrouting','handle',handle],check=True)
def install(ips):
    remove();addresses='{ '+', '.join(ips)+' }'
    rule=f'insert rule inet kindnet-network-policies postrouting ip saddr {addresses} ip daddr {addresses} ct direction reply ct state established counter accept comment "{COMMENT}"\n'
    (ROOT/'established-reply-mitigation.nft').write_text(rule)
    for node in nodes:subprocess.run(['docker','exec','-i',node,'nft','-f','-'],input=rule,text=True,check=True)
def snapshot():
    return {node:subprocess.check_output(['docker','exec',node,'nft','-a','list','chain','inet','kindnet-network-policies','postrouting'],text=True) for node in nodes}
def isolation_checks():
    pods=kjson(['-n',NS,'get','pods','-o','json'])['items']
    target=next(p for p in pods if p['metadata']['name']=='mochi-validation-0')
    source=next(p for p in pods if p['metadata']['name']=='mochi-validation-1')
    ip=target['status']['podIP'];node=source['spec']['nodeName']
    cid=source['status']['containerStatuses'][0]['containerID'].split('://',1)[1]
    details=json.loads(subprocess.check_output(['docker','exec',node,'crictl','inspect',cid]));pid=str(details['info']['pid'])
    commands={
        'outside_allowed_mqtt':K+['-n','mochi','exec','mochi-loadtest','--','sh','-c',f'timeout 3 nc -w 2 {ip} 1883 < /dev/null'],
        'outside_denied_registry':K+['-n','mochi','exec','mochi-loadtest','--','sh','-c',f'timeout 3 nc -w 2 {ip} 7947 < /dev/null'],
        'inside_denied_new_port':['docker','exec',node,'nsenter','-t',pid,'-n','setpriv','--reuid=65532','--regid=65532','--clear-groups','timeout','2','bash','-c',f'exec 3<>/dev/tcp/{ip}/1884'],
    }
    out={}
    for name,cmd in commands.items():
        started=time.monotonic();p=subprocess.run(cmd,capture_output=True,text=True,timeout=6)
        out[name]={'exit_code':p.returncode,'seconds':time.monotonic()-started,'stdout':p.stdout,'stderr':p.stderr,'command':cmd}
    (ROOT/f'mitigation-isolation-{VARIANT or "debug"}.json').write_text(json.dumps(out,indent=2)+'\n')
    return out
report=[]
try:
    ips=[p['status']['podIP'] for p in kjson(['-n',NS,'get','pods','-o','json'])['items']]
    for phase,count in [('before',3 if VARIANT else 2),('patched',3),('restored',3 if VARIANT else 2)]:
        if phase=='patched':install(ips)
        else:remove()
        if phase=='patched' and VARIANT:isolation_checks()
        for attempt in range(count):
            label=f'established-reply-{VARIANT+"-" if VARIANT else ""}{phase}-{attempt}';before=snapshot()
            if phase=='patched' and not all(COMMENT in v for v in before.values()):raise RuntimeError('policy sync removed mitigation')
            r=workload(label,['-connections','6000','-concurrency','256','-subscribe'])
            report.append({'phase':phase,'before':before,'after':snapshot(),'result':r})
            (ROOT/f'reply-mitigation-comparison{"-"+VARIANT if VARIANT else ""}.json').write_text(json.dumps(report,indent=2)+'\n')
finally:remove()
