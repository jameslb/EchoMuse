"""No microphone access: prove filesystem/permit readiness before a pilot."""
import json
from pathlib import Path
import sys
import unittest
import re
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch
from aiohttp import web
from aiohttp.test_utils import make_mocked_request
from websockets.asyncio.server import ServerConnection
from websockets.server import ServerProtocol
from websockets.protocol import State
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import em_realtime_bootstrap as module
import test_aec_export as export_tests

def status(mode='700', file_mode='600', names='permit.local.json', wake=(True,True), **changes):
    permit={'approved':True,'scope':'one-dot-aec-uplink','expires_at':1100,'maximum_seconds':20}
    permit.update(changes)
    values=['"wakeWordEnabled":'+json.dumps(v) for v in wake]
    return '\n'.join((mode,file_mode,'1000',names,*values,json.dumps(permit)))

class ReadinessTests(unittest.TestCase):
    def test_actual_0755_directory_rejected_despite_valid_armed_permit(self):
        result=module.validate_aec_readiness(status(mode='755'))
        self.assertFalse(result['ready'])
        self.assertEqual(result['reason'],'private_directory_required')

    def test_exact_private_fresh_permit_ready(self):
        result=module.validate_aec_readiness(status())
        self.assertTrue(result['ready'])
        self.assertEqual(result['expires_at'],1100)
        self.assertEqual(result['checked_device_unix_s'],1000)

    def test_expired_future_overbroad_or_ambiguous_permits_rejected(self):
        for changes in ({'expires_at':999},{'expires_at':1301},{'approved':False},
                        {'maximum_seconds':21},{'scope':'other'},{'extra':'data'}):
            self.assertFalse(module.validate_aec_readiness(status(**changes))['ready'])
        for raw in (status(file_mode='644'),status(names='permit.local.json\naec.s16le'),
                    '',status()+' {}',status()+'x'*5000):
            self.assertFalse(module.validate_aec_readiness(raw)['ready'])

    def test_received_and_applied_wake_must_both_be_enabled(self):
        for wake in ((False,True),(True,False),(True,),(),(True,True,True)):
            with self.subTest(wake=wake):
                self.assertFalse(module.validate_aec_readiness(status(wake=wake))['ready'])

class ReadinessShell:
    async def send(self, command):
        marker = re.search(r'__AEC_READY_[a-f0-9]+__',command)[0]
        self.command = command
        raw = (status()+'\n'+marker+'\n').encode()
        self.parts = [raw[:30],raw[30:]]
    async def recv(self): return self.parts.pop(0)

class ReadinessHandlerTests(unittest.IsolatedAsyncioTestCase):
    invoke = export_tests.HandlerTests.invoke
    async def asyncSetUp(self):
        await export_tests.HandlerTests.asyncSetUp(self)
        self.request=make_mocked_request('GET','/api/phase5/aec-readiness/synthetic',app=self.app)
        match=await self.app.router.resolve(self.request)
        self.request._match_info=match; self.handler=match.handler
        self.shell=ReadinessShell()
        self.api._get_device_shell_ws.return_value=self.shell
        self.api._devices['synthetic']=SimpleNamespace(wake_word_enabled=True,
            control_ws=ServerConnection(ServerProtocol(state=State.OPEN),SimpleNamespace()))
        self.api.db=SimpleNamespace(get_wake_word_enabled=lambda device: True)

    async def test_disabled_live_or_persisted_wake_is_not_ready(self):
        # Isolate controller checks from the device wire format. This returned
        # ready before the fix even with a disabled persisted/live wake state.
        device={'ready':True,'device_received_enabled':True,'device_applied_enabled':True}
        with patch.object(module,'_aec_readiness',AsyncMock(return_value=device)):
            self.api._devices['synthetic'].wake_word_enabled=False
            self.assertFalse(json.loads((await self.invoke()).body)['ready'])
            self.api._devices['synthetic'].wake_word_enabled=True
            self.api.db.get_wake_word_enabled=lambda device: False
            self.assertFalse(json.loads((await self.invoke()).body)['ready'])

    async def test_replaced_control_connection_is_not_ready(self):
        async def replaced(ws):
            self.api._devices['synthetic'].control_ws=ServerConnection(
                ServerProtocol(state=State.OPEN),SimpleNamespace())
            return {'ready':True,'device_received_enabled':True,'device_applied_enabled':True}
        with patch.object(module,'_aec_readiness',replaced):
            self.assertFalse(json.loads((await self.invoke()).body)['ready'])

    async def test_non_open_real_connection_is_not_ready(self):
        for state in (State.CONNECTING,State.CLOSING,State.CLOSED):
            with self.subTest(state=state):
                self.api._devices['synthetic'].control_ws.protocol.state=state
                self.assertFalse(json.loads((await self.invoke()).body)['ready'])

    async def test_disconnect_during_readback_is_not_ready(self):
        original=self.shell.send
        async def disconnected(command):
            await original(command)
            self.api._devices.pop('synthetic')
        self.shell.send=disconnected
        self.assertFalse(json.loads((await self.invoke()).body)['ready'])

    async def test_authenticated_read_only_metadata_and_release(self):
        response=await self.invoke()
        self.assertTrue(json.loads(response.body)['ready'])
        wake=json.loads(response.body)['wake_readiness']
        self.assertEqual(wake['source'],'authenticated_controller_and_device')
        self.assertTrue(wake['device_applied_enabled'])
        self.assertTrue(wake['persisted_enabled'])
        self.assertEqual(response.headers['Cache-Control'],'no-store')
        self.api._release_shell_ws.assert_awaited_once()
        self.assertIn('stat',self.shell.command)
        self.assertNotIn('chmod',self.shell.command)
        self.assertNotIn(' > ',self.shell.command)
        self.assertIn('/tmp/em-config.json',self.shell.command)

    async def test_no_anonymous_or_readonly_access(self):
        for role in (None,'readonly'):
            response=await self.invoke(role)
            self.assertIn(response.status,(401,403))
        self.api._get_device_shell_ws.assert_not_awaited()

    async def test_oversize_fails_without_leaking_bytes(self):
        for raw in (b'x'*8193,):
            self.shell.recv=AsyncMock(return_value=raw)
            with self.assertRaises(web.HTTPBadGateway): await self.invoke()
        self.api._release_shell_ws.assert_awaited_once()

if __name__=='__main__':unittest.main()
