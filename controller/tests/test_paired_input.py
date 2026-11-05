"""Actual controller hook tests, fake links and clock; no audio/device/service I/O."""
import asyncio,json,sys,unittest
from pathlib import Path
from types import SimpleNamespace
import numpy as np
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from em_paired_input import PairedInput
from em_paired_codec import Context,encode
import em_realtime as rt

class PairTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.now=0;self.allowed=True;self.sent=[]
        async def send(message):self.sent.append(message)
        self.pair=PairedInput(send,lambda:self.allowed,lambda:self.now)
        self.context=Context(b'1'*16,7,9,3,b'B'*32,domain='unity_f32_s24',load_status='loaded',hardware_reference=True)
        self.event=dict(type='pair_ready',stream=self.context.stream.hex(),epoch=7,session=9,generation=3,
            bank_sha256=self.context.bank_sha256.hex(),domain='unity_f32_s24',load_status='loaded',hardware_reference=True,
            source_now_ns=1_000_000,encoding='unity_f32_from_gained_s16_v1',clock_kind='mic_observation_v1')
    async def begin(self):await self.pair.begin(stream=self.context.stream,epoch=7,session=9,lease_deadline_ns=1_000_000_000)
    async def ready(self):
        await self.begin();self.now=10_000_000;await self.pair.ready(self.event)
    def frame(self,seq=0,context=None):
        c=context or self.context
        return encode(c,seq,1234+512*seq,1_000_000,np.full(512,.25,dtype='<f4'),np.full(512,-123,dtype='<i2'))
    async def test_two_phase_no_buffer_before_ready_and_fractional_roundtrip(self):
        await self.begin();self.assertFalse(self.pair.frame(self.frame()));self.assertIsNone(self.pair.receiver)
        self.now=10_000_000;await self.pair.ready(self.event)
        self.assertEqual([x['type'] for x in self.sent],['pair_open','pair_ack'])
        self.assertTrue(self.pair.frame(self.frame()));mic,ref=self.pair.pop()
        self.assertTrue(np.all(mic==.25));self.assertTrue(np.all(ref==-123))
    async def test_missing_filter_and_unknown_encoding_never_ack(self):
        for changes in [dict(load_status='missing'),dict(hardware_reference=False),dict(encoding='unknown')]:
            self.pair=PairedInput(self.pair.send,lambda:True,lambda:self.now)
            await self.begin();self.now+=1
            with self.assertRaises(ValueError):await self.pair.ready(dict(self.event,**changes))
            self.assertTrue(self.pair.retired)
        self.assertNotIn('pair_ack',[x['type'] for x in self.sent])
    async def test_timeout_clock_rollback_and_no_rearm(self):
        await self.begin();self.now=250_000_001
        with self.assertRaises(PermissionError):self.pair.pop()
        with self.assertRaises(PermissionError):await self.begin()
        self.pair=PairedInput(self.pair.send,lambda:True,lambda:self.now)
        await self.begin();self.now-=1
        with self.assertRaises(PermissionError):self.pair.pop()
    async def test_mute_link_loss_and_expiry_purge_before_consumption(self):
        for mode in ['mute','link','expiry']:
            self.now=0;self.allowed=True;self.pair=PairedInput(self.pair.send,lambda:self.allowed,lambda:self.now)
            await self.ready();self.pair.frame(self.frame())
            if mode=='expiry':self.now=1_000_000_000
            else:self.allowed=False
            with self.assertRaises(PermissionError):self.pair.pop()
            self.assertFalse(self.pair.receiver.pending)
    async def test_old_receipt_and_duplicate_do_not_rearm_or_poison(self):
        await self.begin();self.assertFalse(await self.pair.ready(dict(self.event,epoch=6)))
        self.now=10_000_000;await self.pair.ready(self.event);self.pair.frame(self.frame())
        with self.assertRaises(ValueError):self.pair.frame(self.frame())
        self.assertIsNotNone(self.pair.pop())
    async def test_gap_and_overflow_fail_closed(self):
        await self.ready();self.pair.frame(self.frame())
        with self.assertRaises(ValueError):self.pair.frame(self.frame(2))
        self.assertTrue(self.pair.retired)
    async def test_control_send_failure_retires_negotiation(self):
        async def fail(_):raise ConnectionError('fake closed control socket')
        self.pair=PairedInput(fail,lambda:True,lambda:self.now)
        with self.assertRaises(ConnectionError):await self.begin()
        self.assertTrue(self.pair.retired)
    async def test_idle_after_drain_and_closed_receipt_retire(self):
        await self.ready();self.pair.frame(self.frame());self.pair.pop();self.now+=250_000_001
        with self.assertRaises(PermissionError):self.pair.pop()
        self.now=0;self.pair=PairedInput(self.pair.send,lambda:True,lambda:self.now)
        await self.ready()
        self.assertFalse(self.pair.closed(dict(self.event,epoch=6)))
        self.assertTrue(self.pair.closed(dict(self.event,type='pair_closed')))
        self.assertTrue(self.pair.retired)
    async def test_five_frame_native_burst_consumed_without_unbounded_queue(self):
        await self.ready()
        for seq in range(5):
            self.pair.frame(self.frame(seq));self.assertIsNotNone(self.pair.pop())
        self.assertFalse(self.pair.retired)

    async def test_dispatch_yields_to_notified_consumer_before_fifth_period(self):
        await self.ready()
        started=asyncio.Event()
        async def consumer():
            await self.pair.wait_available()
            self.assertIsNotNone(self.pair.pop())
            started.set()
        task=asyncio.create_task(consumer())
        await asyncio.sleep(0)  # consumer is waiting before the native burst
        binding=object.__new__(rt.Binding);binding.paired=self.pair
        async def close():pass
        binding.close=close
        try:
            accepted=[await binding.paired_frame(self.frame(i)) for i in range(5)]
            self.assertEqual(accepted,[True]*5)
            self.assertTrue(started.is_set())
            self.assertEqual(len(self.pair.receiver.pending),4)
        finally:
            task.cancel();await asyncio.gather(task,return_exceptions=True)
    async def test_only_new_wake_can_explicitly_rearm_after_retirement(self):
        await self.ready();self.pair.frame(self.frame());self.pair.revoke()
        with self.assertRaises(PermissionError):self.pair.rearm_for_wake(9)
        self.pair.rearm_for_wake(10)
        self.assertIsNone(self.pair.receiver)
        with self.assertRaises(ValueError):await self.begin()
        await self.pair.begin(stream=b'2'*16,epoch=7,session=10,lease_deadline_ns=1_000_000_000)
        self.assertFalse(await self.pair.ready(self.event))

class BindingPairTests(unittest.IsolatedAsyncioTestCase):
    async def test_default_off_and_existing_privacy_links(self):
        class Socket:
            async def send(self,_):pass
        class Route:
            def __init__(self,*args,**kw):self.options=kw
            async def close(self):pass
            async def control(self,event):pass
        d=SimpleNamespace(device_id='paired-fixture',secure=True,capabilities=['oww_local_only',*rt.CAPABILITIES,rt.PAIRED_CAPABILITY],
            control_ws=Socket(),data_ws=Socket(),listen_view=SimpleNamespace(private=True),muted=False,wake_word_enabled=True,
            voice_lock=asyncio.Lock(),private_turn_task=None,speaking=False,oww_paused=asyncio.Event())
        async def claim(*_):return True
        off=rt.Binding(d,Route,claim);self.assertNotIn(rt.PAIRED_CAPABILITY,off.capabilities());await off.close()
        on=rt.Binding(d,Route,claim,paired_input=True,strict_wake=True);on.session=9;on.data_ws=d.data_ws
        self.assertIn(rt.PAIRED_CAPABILITY,on.capabilities())
        await on.paired.begin(stream=b'1'*16,epoch=7,session=9,lease_deadline_ns=on.paired.clock()+1_000_000_000)
        d.data_ws=Socket()
        with self.assertRaises(PermissionError):on.paired.pop()
        self.assertTrue(on.paired.retired);await on.close()
    async def test_mute_revoke_precedes_route_callback(self):
        # Exercise the real control hook without changing native/mic behavior.
        from unittest.mock import AsyncMock
        binding=object.__new__(rt.Binding);binding.paired=SimpleNamespace(revoke=lambda:order.append('revoke'))
        binding.closed=False;binding.session=9;binding.owned_once=False
        order=[]
        async def control(event):order.append('route')
        binding.route=SimpleNamespace(control=control)
        await binding.control(dict(type='mute_state',muted=True))
        self.assertEqual(order,['revoke','route'])

if __name__=='__main__':unittest.main()
