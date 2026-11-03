"""OFFLINE prototype; not registered on any live device/controller transport.

The caller must supply existing authenticated link/lease/mute state and a verified
source-clock mapping. This module cannot authenticate a peer or create a lease.
Loaded-bank metadata is eligibility evidence, not proof of acoustic readiness.
"""
from dataclasses import dataclass
from collections import deque
import struct
import numpy as np

CAPABILITY='phase5_paired_aec_input_v1'
SAMPLES=512
HEADER=struct.Struct('!4s16sQIIQQI')
MAGIC=b'P5I1'
WIRE_BYTES=HEADER.size+SAMPLES*6

@dataclass(frozen=True)
class Context:
    stream: bytes
    epoch: int
    session: int
    generation: int
    bank_sha256: bytes
    first_sample: int=0
    domain: str='unknown'
    load_status: str='unknown'
    hardware_reference: bool=False

    def validate(self):
        if (type(self.stream) is not bytes or len(self.stream)!=16 or not any(self.stream)
            or type(self.bank_sha256) is not bytes or len(self.bank_sha256)!=32 or not any(self.bank_sha256)
            or self.domain!='unity_f32_s24' or self.load_status!='loaded'
            or self.hardware_reference is not True):
            raise ValueError('Ineligible paired-input context')
        for value,upper in [(self.epoch,2**64),(self.session,2**32),(self.generation,2**32)]:
            if type(value) is not int or not 0<value<upper:raise ValueError('Invalid identity counter')
        if type(self.first_sample) is not int or not 0<=self.first_sample<2**64-SAMPLES:
            raise ValueError('Invalid sample counter')

def encode(context,sequence,sample_start,captured_ns,residual,reference):
    context.validate()
    if (type(sequence) is not int or not 0<=sequence<2**32
        or type(sample_start) is not int or not 0<=sample_start<2**64-SAMPLES
        or type(captured_ns) is not int or not 0<=captured_ns<2**64):
        raise ValueError('Invalid frame counters')
    if (residual.shape!=(SAMPLES,) or residual.dtype!=np.dtype('<f4')
        or reference.shape!=(SAMPLES,) or reference.dtype!=np.dtype('<i2')
        or not np.isfinite(residual).all()):
        raise ValueError('Expected512 finite float residual and S16 reference samples')
    return HEADER.pack(MAGIC,context.stream,context.epoch,context.session,sequence,
                       sample_start,captured_ns,context.generation)+residual.tobytes()+reference.tobytes()

class Receiver:
    """Fail-closed bounded queue; no file/audio/network or live-control operations."""
    def __init__(self,context,*,authenticated,negotiated,lease_deadline_ns,now_ns,max_age_ns=250_000_000,capacity=4):
        context.validate()
        if authenticated is not True or negotiated is not True:raise PermissionError('Unnegotiated link')
        if (type(now_ns) is not int or type(lease_deadline_ns) is not int or not 0<=now_ns<lease_deadline_ns
            or lease_deadline_ns-now_ns>30_000_000_000
            or type(max_age_ns) is not int or not 0<max_age_ns<=250_000_000
            or type(capacity) is not int or not 1<=capacity<=4):raise ValueError('Invalid bounds')
        self.context=context;self.deadline=lease_deadline_ns;self.age=max_age_ns;self.capacity=capacity
        self.next_sequence=0;self.next_sample=context.first_sample;self.pending=deque();self.closed=False
        self.last_now=now_ns

    def close(self):
        self.closed=True;self.pending.clear()

    def _privacy(self,now_ns,muted):
        if (self.closed or muted is not False or type(now_ns) is not int
            or not self.last_now<=now_ns<self.deadline):
            self.close();raise PermissionError('Paired input closed')
        self.last_now=now_ns

    def push(self,packet,*,now_ns,muted=False):
        self._privacy(now_ns,muted)
        if len(packet)!=WIRE_BYTES:raise ValueError('Invalid paired frame size')
        magic,stream,epoch,session,sequence,sample,captured,generation=HEADER.unpack_from(packet)
        c=self.context
        if magic!=MAGIC:raise ValueError('Unknown paired format')
        if (stream,epoch,session,generation)!=(c.stream,c.epoch,c.session,c.generation):
            raise ValueError('Stale or foreign frame')
        if sequence<self.next_sequence:raise ValueError('Duplicate frame')
        if sequence!=self.next_sequence or sample!=self.next_sample:
            self.close();raise ValueError('Input gap requires a new context')
        if not 0<=now_ns-captured<=self.age:
            self.close();raise ValueError('Stale or future capture')
        if len(self.pending)>=self.capacity:
            self.close();raise BufferError('Bounded input overflow')
        residual=np.frombuffer(packet,dtype='<f4',count=SAMPLES,offset=HEADER.size).copy()
        reference=np.frombuffer(packet,dtype='<i2',count=SAMPLES,offset=HEADER.size+SAMPLES*4).copy()
        if not np.isfinite(residual).all():
            self.close();raise ValueError('Nonfinite residual')
        self.pending.append((captured,residual,reference));self.next_sequence+=1;self.next_sample+=SAMPLES

    def pop(self,*,now_ns,muted=False):
        self._privacy(now_ns,muted)
        if not self.pending:return None
        captured,residual,reference=self.pending[0]
        if not 0<=now_ns-captured<=self.age:
            self.close();raise ValueError('Queued input became stale')
        self.pending.popleft();return residual,reference
