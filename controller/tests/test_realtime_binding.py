"""Opt-in route faults; fake sockets only, no devices or microphone access."""
import asyncio
import json
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import em_realtime as rt


class Socket:
    def __init__(self):
        self.sent=[]
        self.fail=False
    async def send(self,value):
        if self.fail:raise ConnectionError('injected send failure')
        self.sent.append(value)


class Route:
    def __init__(self,control,data,**options):
        self.send_control,self.send_data=control,data
        self.options=options
        self.wakes=[]
        self.frames=[]
        self.controls=[]
        self.closed=False
        self.ready=asyncio.Event()
        self.hardware_closed=asyncio.Event()
    async def wake(self,event):
        self.wakes.append(event)
        await self.ready.wait()
        return True
    async def frame(self,packet):self.frames.append(packet)
    async def control(self,event):
        self.controls.append(event)
        if event['type']=='rt_ready':self.ready.set()
        if event['type']=='rt_closed':self.hardware_closed.set()
    async def close(self):self.closed=True
    async def wait_closed(self):await self.hardware_closed.wait()


async def claim(device,event):return True


class BindingTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.device=SimpleNamespace(device_id='test-dot',secure=True,
            capabilities=['oww_local_only',*rt.CAPABILITIES],
            control_ws=Socket(),data_ws=Socket(),listen_view=SimpleNamespace(private=True),
            muted=False,wake_word_enabled=True,voice_lock=asyncio.Lock(),
            private_turn_task=None,speaking=False,oww_paused=asyncio.Event())
        self.binding=rt.Binding(self.device,Route,claim)
    async def asyncTearDown(self):
        await self.binding.close()
        replacement=getattr(self.device,'realtime',None)
        if replacement is not None and replacement is not self.binding:
            await replacement.close()
        rt._factories.clear()
    async def wake(self,session=7):
        result=await self.binding.control({'type':'oww_wake','session':session,'ageMs':0})
        await asyncio.sleep(0)
        return result

    async def test_attach_requires_explicit_registration_tls_and_both_capabilities(self):
        self.assertIsNone(rt.attach(self.device,claim))
        rt.register('test-dot',Route)
        with self.assertRaises(ValueError):rt.register('test-dot',Route)
        self.device.secure=False
        self.assertIsNone(rt.attach(self.device,claim))
        self.device.secure=True
        self.device.capabilities.remove(rt.CAPABILITIES[1])
        self.assertIsNone(rt.attach(self.device,claim))
        self.device.capabilities.append(rt.CAPABILITIES[1])
        attached=rt.attach(self.device,claim)
        self.assertIsNotNone(attached)
        await attached.close()

    async def test_waiting_wake_does_not_block_receipt_reader_or_duplicate_owner(self):
        self.assertTrue(await self.wake())
        self.assertFalse(self.binding.wake_task.done())
        self.assertTrue(await self.wake())
        await self.binding.control({'type':'rt_ready'})
        await self.binding.wake_task
        self.assertEqual(len(self.binding.route.wakes),1)
        self.assertEqual(self.binding.route.options['private_state'],'local')
        self.assertTrue(self.binding.route.options['exclusive'])

    async def test_native_busy_disabled_wake_and_mute_keep_existing_owner(self):
        for field in ('muted','speaking'):
            setattr(self.device,field,True)
            self.assertFalse(await self.wake())
            self.assertFalse(await self.binding.frame(b'fixture'))
            setattr(self.device,field,False)
        self.device.wake_word_enabled=False
        self.assertFalse(await self.wake())
        self.device.wake_word_enabled=True
        await self.device.voice_lock.acquire()
        self.assertFalse(await self.wake())
        self.device.voice_lock.release()
        self.device.oww_paused.set()
        self.assertFalse(await self.wake())
        self.assertEqual(self.binding.route.wakes,[])

    async def test_arbitration_loss_does_not_start_route(self):
        async def lost(device,event):return False
        self.binding.claim_wake=lost
        await self.wake()
        await self.binding.wake_task
        self.assertEqual(self.binding.route.wakes,[])
        self.assertEqual(json.loads(self.device.control_ws.sent[-1])['type'],'listen_close')
        self.assertIsNone(self.binding.session)

    async def test_native_handover_waits_for_hardware_after_button_close(self):
        await self.wake()
        await self.binding.control({'type':'rt_ready'})
        await self.binding.wake_task
        await self.binding.control({'type':'button'})
        self.assertTrue(self.binding.closed)
        self.assertTrue(self.binding.requires_handover)
        task=asyncio.create_task(self.binding.handover())
        await asyncio.sleep(0)
        self.assertFalse(task.done())
        await self.binding.control({'type':'rt_closed'})
        await task
        self.assertFalse(self.binding.requires_handover)

    async def test_data_send_failure_and_connection_replacement_propagate(self):
        self.binding.data_ws=self.device.data_ws
        self.device.data_ws.fail=True
        with self.assertRaises(ConnectionError):await self.binding.send_data(b'\x09test')
        self.device.data_ws=Socket()
        with self.assertRaises(ConnectionError):await self.binding.send_data(b'\x09test')
        self.assertEqual(self.device.data_ws.sent,[])
        self.assertFalse(await self.binding.frame(b'fixture'))
        self.assertTrue(self.binding.closed)

    async def test_control_send_failure_and_retired_link_propagate(self):
        self.device.control_ws.fail=True
        with self.assertRaises(ConnectionError):
            await self.binding.send_control({'type':'rt_close'})
        self.device.control_ws=Socket()
        with self.assertRaises(ConnectionError):
            await self.binding.send_control({'type':'rt_close'})
        self.assertEqual(self.device.control_ws.sent,[])

    async def test_pre_wake_frames_remain_exclusively_in_route(self):
        self.assertTrue(await self.binding.frame(b'fixture'))
        self.assertEqual(self.binding.route.frames,[b'fixture'])
        self.assertFalse(self.binding.active)
        self.assertEqual(self.device.control_ws.sent,[])

    async def test_other_pending_session_is_closed_not_silently_dropped(self):
        await self.wake(7)
        await self.wake(8)
        self.assertEqual(len(self.binding.route.wakes),1)
        message=json.loads(self.device.control_ws.sent[-1])
        self.assertEqual((message['type'],message['session']),('listen_close',8))
        self.assertEqual(self.binding.session,7)

    async def test_mute_during_arbitration_prevents_backend_open(self):
        allowed=asyncio.Event()
        async def slow(device,event):
            await allowed.wait()
            return True
        self.binding.claim_wake=slow
        await self.wake()
        self.device.muted=True
        allowed.set()
        await self.binding.wake_task
        self.assertEqual(self.binding.route.wakes,[])

    async def test_native_handover_rearms_on_new_private_frame_only_after_native_idle(self):
        await self.wake()
        await self.binding.control({'type':'rt_ready'})
        await self.binding.wake_task
        self.binding.route.hardware_closed.set()
        await self.binding.handover()
        self.device.speaking=True
        self.assertFalse(await self.binding.frame(b'new-private-frame'))
        self.assertIsNone(getattr(self.device,'realtime',None))
        self.device.speaking=False
        self.assertTrue(await self.binding.frame(b'new-private-frame'))
        replacement=self.device.realtime
        self.assertIsNot(replacement,self.binding)
        self.assertEqual(replacement.route.frames,[b'new-private-frame'])
        self.assertFalse(replacement.active)
        self.assertEqual(replacement.route.wakes,[])

    async def test_unmute_and_reconnect_cannot_rearm_retired_route(self):
        self.binding.data_ws=self.device.data_ws
        await self.binding.close()
        self.binding.handover_done=True
        await self.binding.control({'type':'mute_state','muted':False})
        self.assertIsNone(getattr(self.device,'realtime',None))
        self.device.data_ws=Socket()
        self.assertFalse(await self.binding.frame(b'new-private-frame'))
        self.assertIsNone(getattr(self.device,'realtime',None))

    async def test_strict_policy_survives_missing_capabilities_and_closed_binding(self):
        rt.register('test-dot',Route,strict_wake=True)
        self.assertFalse(rt.native_wake_allowed('test-dot'))
        self.assertTrue(rt.native_wake_allowed('another-dot'))
        self.device.capabilities=[]
        self.assertIsNone(rt.attach(self.device,claim))
        self.assertFalse(rt.native_wake_allowed('test-dot'))
        self.binding.strict_wake=True
        await self.binding.close()
        self.assertTrue(await self.wake(9))
        self.assertEqual(json.loads(self.device.control_ws.sent[-1])['reason'],'candidate_not_ready')
        self.assertTrue(await self.binding.frame(b'private-frame'))
        self.assertEqual(self.binding.route.frames,[])

    async def test_strict_muted_or_unknown_wakes_never_reach_native(self):
        self.binding.strict_wake=True
        for muted,private in [(True,True),(False,False)]:
            self.device.muted,self.device.listen_view.private=muted,private
            self.assertTrue(await self.wake(10))
            self.assertEqual(self.binding.route.wakes,[])
            self.assertTrue(await self.binding.frame(b'private-frame'))
        self.assertEqual(self.binding.route.frames,[])

    async def test_state_loss_after_ownership_still_retires_route(self):
        await self.wake()
        await self.binding.control({'type':'rt_ready'})
        await self.binding.wake_task
        await self.binding.control({'type':'listen_state','state':'unknown'})
        self.assertTrue(self.binding.closed)


if __name__=='__main__':unittest.main()
