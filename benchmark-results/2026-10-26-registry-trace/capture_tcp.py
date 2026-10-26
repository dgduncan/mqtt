"""Capture only broker registry/forwarding and Raft TCP headers on a Docker node."""
import signal,socket,struct,sys,time
running=True

def stop(*unused):
    global running
    running=False
signal.signal(signal.SIGTERM,stop)
s=socket.socket(socket.AF_PACKET,socket.SOCK_RAW,socket.htons(3))
s.bind(('eth0',0));s.settimeout(.5)
out=sys.stdout.buffer
out.write(struct.pack('<IHHIIII',0xa1b2c3d4,2,4,0,0,96,1));out.flush()
count=0
while running:
    try:frame=s.recv(65535)
    except socket.timeout:continue
    if len(frame)<54 or frame[12:14]!=b'\x08\x00' or frame[23]!=6:continue
    ihl=(frame[14]&15)*4;offset=14+ihl
    if len(frame)<offset+20:continue
    sport,dport=struct.unpack_from('!HH',frame,offset)
    if sport not in (7947,7948) and dport not in (7947,7948):continue
    stamp=time.time_ns();data=frame[:96]
    out.write(struct.pack('<IIII',stamp//1000000000,(stamp%1000000000)//1000,len(data),len(frame))+data)
    count+=1
    if count%128==0:out.flush()
out.flush();s.close()
