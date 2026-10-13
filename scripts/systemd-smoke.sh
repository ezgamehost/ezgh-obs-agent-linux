#!/bin/bash
# Run only on a disposable Linux machine with systemd as PID 1.
set -euo pipefail
cd "$(dirname "$0")/.."
work=$(mktemp -d)
server_pid=
cleanup() {
  systemctl stop ezgh-obs-agent-linux.service || true
  if [ -n "$server_pid" ]; then kill "$server_pid" 2>/dev/null || true; fi
  rm -rf "$work"
}
trap cleanup EXIT
arch=$(dpkg --print-architecture)
dpkg -i "dist/ezgh-obs-agent-linux_0.1.0_${arch}.deb"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$work/key.pem" -out "$work/ca.pem" -days 1 -subj /CN=localhost -addext subjectAltName=DNS:localhost >/dev/null 2>&1
install -m 644 "$work/ca.pem" /etc/ezgh-obs-agent-linux/smoke-ca.pem
python3 - "$work" <<'PY' &
import http.server, ssl, pathlib, sys, json
root=pathlib.Path(sys.argv[1])
class Handler(http.server.BaseHTTPRequestHandler):
 def do_POST(self):
  body=self.rfile.read(int(self.headers['Content-Length']))
  with (root/'received').open('ab') as out: out.write(body)
  self.send_response(200);self.send_header('Content-Type','application/x-protobuf');self.end_headers()
 def log_message(self,*args): pass
server=http.server.HTTPServer(('127.0.0.1',18443),Handler)
context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(root/'ca.pem',root/'key.pem')
server.socket=context.wrap_socket(server.socket,server_side=True);server.serve_forever()
PY
server_pid=$!
printf 'ezgh_ik_%043d\n' 0 > /etc/ezgh-obs-agent-linux/ingest-key
chown root:ezgh-obs-agent /etc/ezgh-obs-agent-linux/ingest-key
chmod 640 /etc/ezgh-obs-agent-linux/ingest-key
cat > /etc/ezgh-obs-agent-linux/config.yaml <<'CONFIG'
version: 1
target: smoke
environment: smoke
endpoint: https://localhost:18443
ca_file: /etc/ezgh-obs-agent-linux/smoke-ca.pem
credential_file: /etc/ezgh-obs-agent-linux/ingest-key
state_dir: /var/lib/ezgh-obs-agent-linux
metrics:
  interval: 1s
logs:
  journal:
    start_at: end
CONFIG
systemd-analyze verify packaging/ezgh-obs-agent-linux.service
systemctl start ezgh-obs-agent-linux.service
sleep 3
printf 'ezgh-native-journal-smoke-marker\n' | systemd-cat -t ezgh-agent-smoke
for attempt in $(seq 1 60); do
 if [ -f "$work/received" ] && grep -aq ezgh-native-journal-smoke-marker "$work/received" && grep -aq node_cpu_seconds_total "$work/received"; then break; fi
 sleep 1
done
grep -aq ezgh-native-journal-smoke-marker "$work/received"
grep -aq node_cpu_seconds_total "$work/received"
pid=$(systemctl show -p MainPID --value ezgh-obs-agent-linux.service)
[ "$(ps -o user= -p "$pid" | xargs)" = ezgh-obs-agent ]
runuser -u ezgh-obs-agent -- ezgh-obs-agent-linux doctor
before=$(cat /var/lib/ezgh-obs-agent-linux/host-id)
systemctl reload ezgh-obs-agent-linux.service
systemctl restart ezgh-obs-agent-linux.service
[ "$(cat /var/lib/ezgh-obs-agent-linux/host-id)" = "$before" ]
dpkg -i "dist/ezgh-obs-agent-linux_0.1.0_${arch}.deb"
systemctl is-active --quiet ezgh-obs-agent-linux.service
dpkg -r ezgh-obs-agent-linux
[ "$(cat /var/lib/ezgh-obs-agent-linux/host-id)" = "$before" ]
echo 'PASS: native journal + host metrics over TLS; non-root systemd; reload/restart/upgrade/uninstall'
