import collections,datetime,gzip,json,pathlib,re,socket,struct
ROOT=pathlib.Path(__file__).resolve().parent

def packets(path):
    if not path.exists():path=path.with_suffix(path.suffix+'.gz')
    opener=gzip.open if path.suffix=='.gz' else open
    with opener(path,'rb') as f:
        header=f.read(24)
        if len(header)!=24:raise ValueError('missing pcap header')
        while True:
            record=f.read(16)
            if len(record)<16:break
            sec,usec,caplen,origlen=struct.unpack('<IIII',record)
            b=f.read(caplen)
            if len(b)<caplen:break
            if len(b)<54 or b[12:14]!=b'\x08\x00':continue
            ihl=(b[14]&15)*4; off=14+ihl
            if b[23]!=6 or len(b)<off+20:continue
            sp,dp,seq,ack=struct.unpack_from('!HHII',b,off)
            tcp_len=(b[off+12]>>4)*4
            prefix=b[off+tcp_len:]
            yield {'time':sec+usec/1e6,'src':socket.inet_ntoa(b[26:30]),'dst':socket.inet_ntoa(b[30:34]),'sport':sp,'dport':dp,'seq':seq,'ack':ack,'flags':b[off+13],'window':struct.unpack_from('!H',b,off+14)[0],'payload_bytes':struct.unpack_from('!H',b,16)[0]-ihl-tcp_len,'prefix':prefix.hex()}

def errors(suffix="packet-debug"):
    found=[]
    for i in range(3):
        for line in (ROOT/f'mochi-cluster-{i}-{suffix}.log').read_text().splitlines():
            try:r=json.loads(line)
            except ValueError:continue
            if r.get('msg')!='registry request failed':continue
            m=re.search(r'(?:read|write|dial) tcp ([\d.]+):(\d+)->([\d.]+):(\d+): (.*)',r['error'])
            if m:found.append({'node':i,'src':m[1],'sport':int(m[2]),'dst':m[3],'dport':int(m[4]),'error':m[5],'log_time':r['time']})
    return found

if __name__=='__main__':
    failures=errors(); flows={}
    wanted={(r['src'],r['sport'],r['dst'],r['dport']) for r in failures}
    counts=collections.Counter()
    for i in range(3):
        for p in packets(ROOT/f'node-{i}-rpc.pcap'):
            counts[(i,p['src'],p['dst'])]+=1
            key=(p['src'],p['sport'],p['dst'],p['dport'])
            reverse=(p['dst'],p['dport'],p['src'],p['sport'])
            if key not in wanted and reverse not in wanted:continue
            flow=key if key in wanted else reverse
            flows.setdefault(flow,{}).setdefault(str(i),[]).append(p)
    report=[]
    for r in failures:
        key=(r['src'],r['sport'],r['dst'],r['dport'])
        entries=flows.get(key,{})
        if not entries:continue
        base=min(p['time'] for ps in entries.values() for p in ps)
        entry=dict(r)
        entry['captures']={i:[dict(p,offset_ms=round((p['time']-base)*1000,3)) for p in ps] for i,ps in entries.items()}
        report.append(entry)
    (ROOT/'failed-rpc-packets.json').write_text(json.dumps(report,indent=2)+'\n')
    print('error flows',len(failures),'captured',len(report))
    for r in report[:3]:
        print(r['sport'],r['log_time'])
        for node,ps in r['captures'].items():
            print('capture node',node)
            for p in ps:print(p['offset_ms'],p['src'],p['sport'],'->',p['dst'],p['dport'],hex(p['flags']),'seq',p['seq'],'ack',p['ack'],'payload',p['payload_bytes'],'prefix',p['prefix'][:12])
