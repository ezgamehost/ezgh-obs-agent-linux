#!/usr/bin/env python3
"""Generate a CycloneDX 1.5 module inventory from Go's resolved module graph."""
import json, subprocess, sys, uuid
from datetime import datetime, timezone
modules = subprocess.check_output(['go', 'list', '-m', '-json', 'all'], text=True)
decoder = json.JSONDecoder(); components = []
while modules.strip():
    m, end = decoder.raw_decode(modules.lstrip()); modules = modules.lstrip()[end:]
    name = m['Path']; version = m.get('Version', sys.argv[1])
    components.append({'type': 'application' if m.get('Main') else 'library',
                       'name': name, 'version': version,
                       'purl': f'pkg:golang/{name}@{version}'})
print(json.dumps({'bomFormat': 'CycloneDX', 'specVersion': '1.5', 'version': 1,
 'serialNumber': f'urn:uuid:{uuid.uuid4()}',
 'metadata': {'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')},
 'components': components}, indent=2))
