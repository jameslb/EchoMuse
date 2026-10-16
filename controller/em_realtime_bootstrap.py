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
    from echomuse_route import EchoRoute
    import em_realtime
    backend = Backend(config['pipeline_id'], speech_host=config['speech_host'],
        speech_port=config['speech_port'],voice=config['voice'],
        engineering_voice=config['engineering_voice'])
    await backend.verify()
    def factory(control, data, **options):
        return EchoRoute(backend.create_conversation, control, data, **options)
    em_realtime.register(config['device_id'], factory)
    return True
