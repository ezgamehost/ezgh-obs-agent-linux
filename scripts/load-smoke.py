#!/usr/bin/env python3
"""Disposable TLS outage/recovery load test. Usage: load-smoke.py BINARY [SECONDS]."""
import http.server, json, os, pathlib, signal, ssl, subprocess, sys, tempfile, threading, time
binary=str(pathlib.Path(sys.argv[1]).resolve()); duration=int(sys.argv[2]) if len(sys.argv)>2 else 60
with tempfile.TemporaryDirectory(prefix='ezgh-load-') as temporary:
 root=pathlib.Path(temporary); received=0; metrics=0; lock=threading.Lock(); started=time.monotonic()
 class Handler(http.server.BaseHTTPRequestHandler):
  def do_POST(self):
   global received, metrics
   body=self.rfile.read(int(self.headers['Content-Length']))
   elapsed=time.monotonic()-started
   if duration/4 < elapsed < duration/2:
    self.send_response(503);self.send_header('Retry-After','1');self.end_headers();return
   with lock:
    if self.path=='/v1/logs': received+=body.count(b'ezgh-load-record:')
    else: metrics+=1
   self.send_response(200);self.send_header('Content-Type','application/x-protobuf');self.end_headers()
  def log_message(self,*args): pass
 subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(root/'tls.key'),'-out',str(root/'ca.pem'),'-days','1','-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
 tls=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);tls.load_cert_chain(root/'ca.pem',root/'tls.key');server.socket=tls.wrap_socket(server.socket,server_side=True)
 threading.Thread(target=server.serve_forever,daemon=True).start()
 (root/'key').write_text('ezgh_ik_'+'a'*43);os.chmod(root/'key',0o600)
 (root/'input.log').touch()
 (root/'config.yaml').write_text(f'''version: 1
target: load-test
environment: load-test
endpoint: https://localhost:{server.server_port}
ca_file: {root}/ca.pem
credential_file: {root}/key
state_dir: {root}/state
storage:
  log_bytes: 8388608
metrics:
  interval: 30s
logs:
  journal:
    enabled: false
  files:
    - paths: [{root}/input.log]
      service: load-test
      start_at: beginning
''')
 log=(root/'agent.stderr').open('w+')
 process=subprocess.Popen([binary,'run','--config',str(root/'config.yaml')],stdout=log,stderr=log)
 peak_rss=0;peak_queue=0;cpu=0;sent=0
 try:
  started=time.monotonic();deadline=started+duration
  with (root/'input.log').open('ab',buffering=0) as source:
   while time.monotonic()<deadline:
    if process.poll() is not None: raise RuntimeError('agent exited during load')
    target=min(duration*1000,int((time.monotonic()-started)*1000))
    if target>sent:
     source.write(b''.join((f'ezgh-load-record:{i:08d}:'.encode()+b'x'*998+b'\n') for i in range(sent,target)))
     sent=target
    stat=pathlib.Path(f'/proc/{process.pid}/stat').read_text().split();cpu=(int(stat[13])+int(stat[14]))/os.sysconf('SC_CLK_TCK')
    for line in pathlib.Path(f'/proc/{process.pid}/status').read_text().splitlines():
     if line.startswith('VmRSS:'): peak_rss=max(peak_rss,int(line.split()[1])*1024)
    db=root/'state/logs.db'
    if db.exists(): peak_queue=max(peak_queue,db.stat().st_size)
    time.sleep(.05)
  drain=time.monotonic()+120
  while received<sent and time.monotonic()<drain:
   if process.poll() is not None: raise RuntimeError('agent exited during recovery')
   time.sleep(.1)
  status=subprocess.run([binary,'doctor','--config',str(root/'config.yaml')],check=True,capture_output=True,text=True)
  report={'duration_seconds':duration,'sent':sent,'received':received,'metric_requests':metrics,'peak_rss_bytes':peak_rss,'cpu_seconds':cpu,'peak_log_database_bytes':peak_queue,'status':json.loads(status.stdout)}
  print(json.dumps(report,indent=2),flush=True)
  assert received==sent, 'lost or duplicated records'
  assert metrics>0, 'no host metrics received'
  assert peak_queue<=8388608, 'queue exceeded hard limit'
  assert peak_rss<150*1024*1024, 'RSS target exceeded'
 finally:
  process.send_signal(signal.SIGTERM)
  try: process.wait(timeout=30)
  except subprocess.TimeoutExpired: process.kill();process.wait();raise
  server.shutdown();log.close()
