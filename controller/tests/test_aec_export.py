"""Bounded transport and admin authorization; no device, inference or audio I/O."""
import asyncio
import base64
import hashlib
import io
import json
from pathlib import Path
import re
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch
import zipfile

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import em_realtime_bootstrap as module
from aiohttp import web
from aiohttp.test_utils import make_mocked_request


class Shell:
    def __init__(self, files):
        self.files, self.messages, self.commands = files, [], []

    async def send(self, command):
        self.commands.append(command)
        name = next(n for n in module.AEC_FILES if '/' + n in command)
        marker = re.search(r'__AEC_END_[a-f0-9]+__', command)[0]
        raw = self.files[name][:module.AEC_FILES[name]+1]
        encoded = base64.encodebytes(raw) + b'\n' + marker.encode() + b'\n'
        # Fragment both base64 quanta and sentinel, as real WS reads may do.
        self.messages = [encoded[i:i+31] for i in range(0, len(encoded), 31)]

    async def recv(self):
        return self.messages.pop(0)


def fixture():
    pcm = b'\x01\0\x02\0\x03\0' * 320
    events = b'{"samples":320}\n'
    manifest = {'version': 1, 'rate': 16000, 'samples': 320,
                'channels': ['selected_pre_aec', 'selected_post_aec', 'hardware_reference'],
                'pcm_sha256': hashlib.sha256(pcm).hexdigest(),
                'events_sha256': hashlib.sha256(events).hexdigest()}
    return {'capture.local.json': json.dumps(manifest).encode(),
            'aec.s16le': pcm, 'events.local.jsonl': events}


class TransferTests(unittest.IsolatedAsyncioTestCase):
    async def test_exact_archive_with_fragmented_sentinel(self):
        files = fixture()
        shell = Shell(files)
        raw = await module._aec_archive(shell)
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            self.assertEqual(set(archive.namelist()), set(files))
            for name, value in files.items():
                self.assertEqual(archive.read(name), value)
        self.assertEqual(len(shell.commands), 3)
        self.assertTrue(all('head -c ' in c and '[ ! -L ' in c for c in shell.commands))

    async def test_oversize_empty_bad_encoding_and_hash_mismatch_fail(self):
        for data in (b'', b'X' * 4097):
            with self.subTest(size=len(data)), self.assertRaises(ValueError):
                await module._read_aec_file(Shell({'capture.local.json': data}), 'capture.local.json')
        ws = SimpleNamespace(send=AsyncMock(), recv=AsyncMock(return_value=b'X' * 12000))
        with self.assertRaisesRegex(ValueError, 'wire bound'):
            await module._read_aec_file(ws, 'capture.local.json')
        files = fixture()
        files['aec.s16le'] = bytes(len(files['aec.s16le']))
        with self.assertRaisesRegex(ValueError, 'inconsistent'):
            await module._aec_archive(Shell(files))

    async def test_missing_sentinel_and_invalid_base64_are_not_success(self):
        ws = Shell({'capture.local.json': b'fixture'})
        original = ws.send
        async def corrupt(command):
            await original(command)
            ws.messages[0] = b'!' + ws.messages[0]
        ws.send = corrupt
        with self.assertRaises(ValueError):
            await module._read_aec_file(ws, 'capture.local.json')
        never = asyncio.Event()
        ws = SimpleNamespace(send=AsyncMock(), recv=never.wait)
        real_wait = asyncio.wait_for
        async def short_wait(awaitable, timeout):
            return await real_wait(awaitable, timeout=0.01)
        with patch.object(asyncio, 'wait_for', short_wait):
            with self.assertRaises(asyncio.TimeoutError):
                await module._read_aec_file(ws, 'capture.local.json')

    async def test_unlisted_path_rejected_before_command(self):
        ws = SimpleNamespace(send=AsyncMock())
        with self.assertRaises(KeyError):
            await module._read_aec_file(ws, '../../config')
        ws.send.assert_not_awaited()


class HandlerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        # Password hashing is not exercised. Keep the real admin decorator and
        # stub only session lookup, so absent/readonly sessions are tested.
        with patch.dict(sys.modules, {'bcrypt': Mock()}):
            import em_auth
        self.auth = em_auth
        self.api = SimpleNamespace(_devices={'synthetic': object()}, _shell_lock={},
            _shell_dashboard={}, _get_device_shell_ws=AsyncMock(return_value=Shell(fixture())),
            _release_shell_ws=AsyncMock())
        self.app = web.Application()
        module.register_aec_export(self.app, self.api, self.auth)
        self.app.freeze()
        self.request = make_mocked_request('GET', '/api/phase5/aec/synthetic', app=self.app)
        match = await self.app.router.resolve(self.request)
        self.request._match_info = match
        self.handler = match.handler

    async def invoke(self, role='admin'):
        user = {'role': role} if role else None
        with patch.object(self.auth, 'resolve_session', AsyncMock(return_value=user)):
            return await self.handler(self.request)

    async def test_admin_required_before_device_access(self):
        for role, status in [(None, 401), ('readonly', 403)]:
            self.assertEqual((await self.invoke(role)).status, status)
        self.api._get_device_shell_ws.assert_not_awaited()
        response = await self.invoke()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.headers['Cache-Control'], 'no-store')
        self.api._release_shell_ws.assert_awaited_once()

    async def test_existing_terminal_not_interrupted(self):
        self.api._shell_dashboard['synthetic'] = object()
        with self.assertRaises(web.HTTPConflict):
            await self.invoke()
        self.api._get_device_shell_ws.assert_not_awaited()

    async def test_failure_and_cancellation_release_owned_shell_same_task(self):
        acquired, released = [], []
        async def acquire(live):
            acquired.append(asyncio.current_task())
            return Shell(fixture())
        async def release(*args):
            released.append(asyncio.current_task())
        self.api._get_device_shell_ws.side_effect = acquire
        self.api._release_shell_ws.side_effect = release
        for error in (ValueError('synthetic corrupt data'), asyncio.CancelledError()):
            with patch.object(module, '_aec_archive', AsyncMock(side_effect=error)):
                with self.assertRaises((web.HTTPBadGateway, asyncio.CancelledError)):
                    await self.invoke()
        self.assertEqual(acquired, released)
        self.assertEqual(len(released), 2)
        self.assertEqual((await self.invoke()).status, 200)  # not left busy

    async def test_overlapping_export_refused_without_queue(self):
        started, finish = asyncio.Event(), asyncio.Event()
        async def held(ws):
            started.set()
            await finish.wait()
            return b'fixture'
        with patch.object(module, '_aec_archive', held):
            first = asyncio.create_task(self.invoke())
            await started.wait()
            with self.assertRaises(web.HTTPConflict):
                await self.invoke()
            finish.set()
            self.assertEqual((await first).status, 200)
        self.api._get_device_shell_ws.assert_awaited_once()


if __name__ == '__main__':
    unittest.main()
