"""Explicit local candidate registration; disabled without private configuration.

The candidate package is installed at build time, never loaded from a remote
URL. Existing TLS device authentication and arbitration remain authoritative.
"""
import ipaddress
import json
import os
from pathlib import Path
import re


def read_config(path):
    config = json.loads(Path(path).read_text(encoding='utf-8'))
    required = {'enabled','device_id','pipeline_id','speech_host','speech_port',
                'voice','engineering_voice','read_only_test_mode'}
    if set(config) != required or config['enabled'] is not True or config['read_only_test_mode'] is not True:
        raise ValueError('Explicit bounded candidate configuration required')
    for key in ('device_id','pipeline_id'):
        if not isinstance(config[key],str) or not re.fullmatch(r'[A-Za-z0-9_-]{1,96}',config[key]):
            raise ValueError('Invalid explicit candidate identity')
    host = config['speech_host']
    local = host in ('core-piper','core-piper.local.hass.io')
    if not local:
        try:
            address = ipaddress.ip_address(host)
            # Limit this private test endpoint to RFC1918 IPv4, not arbitrary
            # Internet, multicast, loopback, link-local or metadata endpoints.
            local = any(address in ipaddress.ip_network(n) for n in
                        ('10.0.0.0/8','172.16.0.0/12','192.168.0.0/16'))
        except ValueError:local = False
    if not local or type(config['speech_port']) is not int or not 1 <= config['speech_port'] <= 65535:
        raise ValueError('Explicit local speech endpoint required')
    if type(config['engineering_voice']) is not bool:
        raise ValueError('Voice substitution must be explicit')
    if config['voice'] is not None and not isinstance(config['voice'],str):
        raise ValueError('Invalid voice identity')
    if host.startswith('core-piper') and not config['engineering_voice']:
        raise ValueError('Piper is an engineering voice, not Biggie')
    return config


async def start():
    path = os.environ.get('ECHOMUSE_REALTIME_CONFIG')
    if not path:return False
    config = read_config(path)
    # These imports succeed only in a deliberately packaged candidate image.
    from ha_dot_backend import Backend
    import em_realtime
    backend = Backend(config['pipeline_id'], speech_host=config['speech_host'],
        speech_port=config['speech_port'],voice=config['voice'],
        engineering_voice=config['engineering_voice'])
    await backend.verify()
    em_realtime.register(config['device_id'], backend.create_route, strict_wake=True)
    return True


# Private diagnostic export only. Uses the existing authenticated device shell
# and admin API; never creates a permit, records, deletes or executes file data.
AEC_ROOT = '/data/local/tmp/phase5-aec.local'
AEC_FILES = {'capture.local.json': 4096, 'aec.s16le': 1_920_000,
             'events.local.jsonl': 512 * 1024}


def validate_aec_readiness(raw):
    """Validate actual device metadata, never a locally asserted armed flag."""
    failed = {'ready': False, 'reason': 'invalid_device_readback'}
    if not isinstance(raw, str) or len(raw) > 4608:
        return failed
    try:
        directory_mode, permit_mode, clock, names, wake_a, wake_b, content = raw.strip().split('\n', 6)
        if directory_mode != '700':
            return {'ready': False, 'reason': 'private_directory_required'}
        if permit_mode != '600' or names != 'permit.local.json':
            return failed
        # The report contains received and applied snapshots. Both must be on;
        # do not depend on JSON member order or expose the rest of the config.
        if [wake_a, wake_b] != ['"wakeWordEnabled":true'] * 2:
            return {'ready': False, 'reason': 'device_wake_not_enabled'}
        now = int(clock)
        permit = json.loads(content)
        if (set(permit) != {'approved','scope','expires_at','maximum_seconds'}
                or permit['approved'] is not True
                or permit['scope'] != 'one-dot-aec-uplink'
                or type(permit['maximum_seconds']) is not int or permit['maximum_seconds'] != 20
                or type(permit['expires_at']) is not int
                or not now < permit['expires_at'] <= now + 300):
            return failed
    except (ValueError, TypeError, KeyError):
        return failed
    return {'ready': True, 'checked_device_unix_s': now,
            'expires_at': permit['expires_at'], 'maximum_seconds': 20,
            'directory_mode': directory_mode, 'permit_mode': permit_mode,
            'device_received_enabled': True, 'device_applied_enabled': True}


async def _aec_readiness(ws):
    import asyncio
    import secrets
    root = AEC_ROOT
    permit = root + '/permit.local.json'
    marker = '__AEC_READY_' + secrets.token_hex(12) + '__'
    # Fixed read-only paths. No permit creation, chmod, capture, or audio output.
    command = (f'if [ -d {root} ] && [ ! -L {root} ] && '
               f'[ -f {permit} ] && [ ! -L {permit} ]; then '
               f"busybox stat -c '%a' {root} {permit}; busybox date +%s; "
               f'busybox ls -A {root}; '
               "busybox grep -o '\"wakeWordEnabled\":[a-z]*' /tmp/em-config.json; "
               f'busybox head -c 4097 {permit}; fi; '
               f"printf '\\n{marker}\\n'\n")
    await ws.send(command)
    output = bytearray()
    sentinel = ('\n' + marker + '\n').encode()
    async def receive():
        while True:
            value = await ws.recv()
            output.extend(value.encode('ascii') if isinstance(value, str) else value)
            if len(output) > 8192:
                raise ValueError('Readiness response exceeds bound')
            if sentinel in output:
                before, after = bytes(output).split(sentinel, 1)
                if after.strip():
                    raise ValueError('Unexpected readiness suffix')
                return validate_aec_readiness(before.decode('ascii'))
    return await asyncio.wait_for(receive(), timeout=10)


async def _read_aec_file(ws, name):
    import asyncio
    import base64
    import secrets
    maximum = AEC_FILES[name]  # fixed allowlist, never interpolate request paths
    path = AEC_ROOT + '/' + name
    marker = '__AEC_END_' + secrets.token_hex(12) + '__'
    # Binary audio is encoded solely for transport over the existing text shell.
    # head bounds even a corrupt/oversized file. Strict decoding rejects errors,
    # echoed commands, missing files and truncated output; none are executed.
    command = (f'[ -d {AEC_ROOT} ] && [ ! -L {AEC_ROOT} ] && '
               f'[ -f {path} ] && [ ! -L {path} ] && '
               f'busybox head -c {maximum+1} {path} | busybox base64; '
               f"printf '\\n{marker}\\n'\n")
    await ws.send(command)
    output = bytearray()
    marker = ('\n' + marker + '\n').encode()
    # Includes normal base64 wrapping, sentinel and a small transport margin.
    wire_limit = ((maximum+3)//3)*4*2 + 512
    async def receive():
        while True:
            message = await ws.recv()
            if isinstance(message, str):
                message = message.encode('ascii')
            if len(output) + len(message) > wire_limit:
                raise ValueError('AEC export exceeds wire bound')
            output.extend(message)
            if marker in output:
                before, after = bytes(output).split(marker, 1)
                if after.strip():
                    raise ValueError('Unexpected trailing shell output')
                value = base64.b64decode(b''.join(before.split()), validate=True)
                if not value or len(value) > maximum:
                    raise ValueError('AEC file empty or exceeds bound')
                return value
    return await asyncio.wait_for(receive(), timeout=10)


async def _aec_archive(ws):
    import hashlib
    import io
    import zipfile
    files = {name: await _read_aec_file(ws, name) for name in AEC_FILES}
    manifest = json.loads(files['capture.local.json'])
    pcm, events = files['aec.s16le'], files['events.local.jsonl']
    version = manifest.get('version')
    first = 'selected_pre_aec' if version == 1 else 'selected_gain_monitor'
    if (version not in (1, 2) or manifest.get('rate') != 16000
            or manifest.get('channels') != [first, 'selected_post_aec', 'hardware_reference']
            or len(pcm) % 6 or manifest.get('samples') != len(pcm)//6
            or hashlib.sha256(pcm).hexdigest() != manifest.get('pcm_sha256')
            or hashlib.sha256(events).hexdigest() != manifest.get('events_sha256')):
        raise ValueError('Incomplete or inconsistent AEC evidence')
    output = io.BytesIO()
    with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_STORED) as archive:
        for name, data in files.items():
            archive.writestr(name, data)
    return output.getvalue()


def register_aec_export(app, api, auth):
    """Candidate-only admin download; one bounded export, no waiting queue."""
    import asyncio
    from aiohttp import web
    busy = False

    @auth.require_admin
    async def export(request):
        nonlocal busy
        device_id = request.match_info['id']
        live = api._devices.get(device_id)
        lock = api._shell_lock.get(device_id)
        if (busy or live is None or device_id in api._shell_dashboard
                or (lock and lock.locked())):
            raise web.HTTPConflict(reason='Diagnostic export or device shell unavailable')
        busy = True
        readiness = request.match_info.route.name == 'phase5-aec-readiness'
        control = getattr(live, 'control_ws', None)
        try:
            async def collect():
                # Acquire and release in the SAME task: existing shell ownership
                # is task-bound. Cancellation must release only our own session.
                try:
                    ws = await api._get_device_shell_ws(live)
                    return await (_aec_readiness(ws) if readiness else _aec_archive(ws))
                finally:
                    await api._release_shell_ws(device_id, live)
            body = await asyncio.wait_for(collect(), timeout=60)
            if readiness:
                import time
                # The same authenticated live connection must survive the
                # readback. A cached HA selection cannot establish these facts.
                if (api._devices.get(device_id) is not live or control is None
                        or getattr(control, 'closed', True)
                        or getattr(live, 'control_ws', None) is not control
                        or getattr(live, 'wake_word_enabled', None) is not True
                        or api.db.get_wake_word_enabled(device_id) is not True):
                    body = {'ready': False, 'reason': 'controller_wake_or_connection_unverified'}
                elif body.get('ready') is True:
                    body['wake_readiness'] = {
                        'source': 'authenticated_controller_and_device',
                        'observed_at': time.time(), 'controller_enabled': True,
                        'persisted_enabled': True,
                        'device_received_enabled': body['device_received_enabled'],
                        'device_applied_enabled': body['device_applied_enabled'],
                        'same_connection': True}
                return web.json_response(body, headers={'Cache-Control': 'no-store'})
            return web.Response(body=body, content_type='application/zip', headers={
                'Content-Disposition': 'attachment; filename="phase5-aec.local.zip"',
                'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff'})
        except (ValueError, asyncio.TimeoutError, OSError, UnicodeError):
            # Never log private file bytes or return partial evidence as success.
            raise web.HTTPBadGateway(reason='AEC export incomplete; no automatic retry')
        finally:
            busy = False

    app.router.add_get('/api/phase5/aec/{id}', export, allow_head=False)
    app.router.add_get('/api/phase5/aec-readiness/{id}', export,
                      name='phase5-aec-readiness', allow_head=False)
