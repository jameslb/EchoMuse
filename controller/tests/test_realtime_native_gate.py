"""Execute the actual native entry-point guard without starting the controller."""
import ast
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest.mock import Mock

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
import em_realtime as rt


class EnteredNative(Exception):
    pass


class Device:
    device_id='synthetic-dot'
    @property
    def mic_queue(self):
        raise EnteredNative('Native processing reached before any HA/model I/O')


class NativeGateTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        tree=ast.parse((ROOT/'em_controller.py').read_text(encoding='utf-8'))
        function=next(n for n in tree.body if isinstance(n,ast.AsyncFunctionDef)
                      and n.name=='_run_voice_locked')
        # Execute the repository function unchanged with inert dependencies.
        module=ast.Module(body=[function],type_ignores=[])
        self.log=Mock()
        scope={'Device':Device,'em_realtime':rt,'log':self.log}
        exec(compile(module,str(ROOT/'em_controller.py'),'exec'),scope)
        self.run_turn=scope['_run_voice_locked']

    def tearDown(self):
        rt._factories.clear()

    async def test_registered_strict_wake_never_enters_native_even_without_binding(self):
        rt.register('synthetic-dot',lambda *a,**k:None,strict_wake=True)
        await self.run_turn(Device(),is_wakeword=True,session=123)
        self.log.warning.assert_called_once()

    async def test_explicit_button_remains_native_with_strict_candidate(self):
        rt.register('synthetic-dot',lambda *a,**k:None,strict_wake=True)
        with self.assertRaises(EnteredNative):
            await self.run_turn(Device(),trigger_label='button',is_wakeword=False)
        self.log.warning.assert_not_called()

    async def test_unregistered_device_retains_native_wake(self):
        with self.assertRaises(EnteredNative):
            await self.run_turn(Device(),is_wakeword=True)
        self.log.warning.assert_not_called()


if __name__=='__main__':unittest.main()
