"""Opt-in paired lifecycle. No microphone command, lease renewal or DSP switch.

Clock mapping bounds software-observation age; it does not certify acoustic age.
The real binding supplies link/lease/mute checks. No factory enables this today.
"""
from dataclasses import replace
import time
from em_paired_codec import Context,Receiver,HEADER,MAGIC,WIRE_BYTES,CAPABILITY

FRAME_TYPE=10

class PairedInput:
    def __init__(self,send,allowed,clock=time.monotonic_ns):
        self.send=send;self.allowed=allowed;self.clock=clock
        self.identity=None;self.receiver=None;self.context=None;self.retired=False
        self.sent=None;self.deadline=None;self.offset=None;self.last=None
        self.ready_received=None
        self.last_frame=None
        self.last_session=None;self.expected_session=None

    def _check(self):
        now=self.clock()
        if (self.retired or not self.allowed() or (self.last is not None and now<self.last)
            or (self.deadline is not None and now>=self.deadline)
            or (self.identity is not None and self.context is None and now-self.sent>250_000_000)
            or (self.ready_received is not None and self.receiver is None and now-self.ready_received>250_000_000)
            or (self.last_frame is not None and now-self.last_frame>250_000_000)):
            self.revoke();raise PermissionError('Paired session retired')
        self.last=now;return now

    async def begin(self,*,stream,epoch,session,lease_deadline_ns):
        now=self._check()
        if self.identity is not None:raise ValueError('Paired negotiation already attempted')
        if self.expected_session is not None and session!=self.expected_session:raise ValueError('Wrong new wake session')
        # Validate shape without claiming the still-unknown filter is ready.
        if (type(stream) is not bytes or len(stream)!=16 or not any(stream)
            or type(epoch) is not int or not 0<epoch<2**64
            or type(session) is not int or not 0<session<2**32):raise ValueError('Invalid paired identity')
        if type(lease_deadline_ns) is not int or not now<lease_deadline_ns<=now+30_000_000_000:
            raise ValueError('Existing bounded lease required')
        self.identity=dict(stream=stream.hex(),epoch=epoch,session=session)
        self.last_session=session
        self.sent=now;self.deadline=lease_deadline_ns
        try:await self.send(dict(type='pair_open',**self.identity))
        except BaseException:self.revoke();raise

    async def ready(self,event):
        now=self._check()
        if self.identity is None or any(event.get(k)!=v for k,v in self.identity.items()):
            return False # retired/foreign receipt does not poison the active session
        if self.context is not None:return False
        try:
            if not 0<=now-self.sent<=250_000_000:raise ValueError('Clock handshake exceeded bound')
            source=event['source_now_ns']
            if event.get('encoding')!='unity_f32_from_gained_s16_v1' or event.get('clock_kind')!='mic_observation_v1':
                raise ValueError('Unknown input precision or clock semantics')
            if type(source) is not int or not 0<=source<2**63:raise ValueError('Invalid source clock')
            context=Context(bytes.fromhex(event['stream']),event['epoch'],event['session'],event['generation'],
                bytes.fromhex(event['bank_sha256']),domain=event['domain'],load_status=event['load_status'],
                hardware_reference=event['hardware_reference'])
            context.validate()
            # Real source event lies between host request and receipt. Taking
            # the lower offset bound gives a conservative upper queue age.
            self.offset=self.sent-source;self.context=context;self.ready_received=now
            await self.send(dict(type='pair_ack',**self.identity))
        except BaseException:self.revoke();raise
        return True

    def frame(self,packet):
        now=self._check()
        if self.context is None:return False # never buffer before authenticated ready
        if len(packet)!=WIRE_BYTES:raise ValueError('Invalid paired payload size')
        if self.receiver is None:
            magic,stream,epoch,session,seq,sample,_stamp,generation=HEADER.unpack_from(packet)
            c=self.context
            if magic!=MAGIC or (stream,epoch,session,generation)!=(c.stream,c.epoch,c.session,c.generation):
                return False
            if seq!=0:self.revoke();raise ValueError('Missing first paired frame')
            self.context=replace(c,first_sample=sample)
            self.receiver=Receiver(self.context,authenticated=True,negotiated=True,
                lease_deadline_ns=self.deadline-self.offset,now_ns=now-self.offset)
        try:self.receiver.push(packet,now_ns=now-self.offset);self.last_frame=now
        except (PermissionError,BufferError):self.revoke();raise
        except ValueError:
            if self.receiver.closed:self.revoke()
            raise
        return True

    def pop(self):
        now=self._check()
        if self.receiver is None:return None
        try:return self.receiver.pop(now_ns=now-self.offset)
        except (PermissionError,ValueError):self.revoke();raise

    def revoke(self):
        self.retired=True
        if self.receiver is not None:self.receiver.close()

    def rearm_for_wake(self,session):
        # Called only after Binding wins a newly validated user wake. Never
        # called on unmute, reconnect, a receipt, timeout or provider retry.
        if type(session) is not int or not 0<session<2**32 or (self.last_session is not None and session<=self.last_session):
            raise PermissionError('New wake required')
        self.revoke();self.identity=None;self.receiver=None;self.context=None
        self.sent=None;self.deadline=None;self.offset=None;self.last=None
        self.ready_received=None;self.last_frame=None;self.expected_session=session
        self.retired=False
        self._check()

    def closed(self,event):
        if self.identity is None or any(event.get(k)!=v for k,v in self.identity.items()):return False
        self.revoke();return True

    async def close(self):
        self.revoke() # no late control write; normal rt_close/lease close revokes device
