"""Opt-in controller dispatch for measured, bounded realtime voice routes.

No config default, listener, credential loading or backend service is added.
A deployment must explicitly register its route factory before device connect.
Only authenticated TLS device links can attach; existing native behavior is the
default. Factories implement EchoRoute's wake/frame/control/close interface.
"""
import asyncio
import json
import logging
import math

CAPABILITIES = ('realtime_playback_v1', 'realtime_pcm_48000_v1')
_factories = {}


def register(device_id, factory, *, strict_wake=False):
    if not isinstance(device_id, str) or not device_id or not callable(factory):
        raise ValueError('Explicit device identity and route factory required')
    if device_id in _factories:
        raise ValueError('Device already registered')
    if type(strict_wake) is not bool:
        raise ValueError('Explicit boolean wake policy required')
    _factories[device_id] = (factory, strict_wake)
    logging.getLogger('echomuse.realtime').info(
        'Candidate registered; strict_wake=%s', strict_wake)


def native_wake_allowed(device_id):
    """A bounded test must never silently fall through to a selected HA route.

    Registration, not live binding health, enforces this. Missing capabilities,
    closed links or negotiation failure must not silently enable cloud STT.
    Explicit native button/announcement handling remains outside this policy.
    """
    registration = _factories.get(device_id)
    return registration is None or not registration[1]


def attach(device, claim_wake):
    registration = _factories.get(device.device_id)
    if (registration is None or not device.secure or
            not set((*CAPABILITIES, 'oww_local_only')).issubset(device.capabilities)):
        return None
    factory, strict_wake = registration
    return Binding(device, factory, claim_wake, strict_wake=strict_wake)


class Binding:
    def __init__(self, device, factory, claim_wake, *, strict_wake=False):
        self.device = device
        self.control_ws = device.control_ws
        self.data_ws = None
        self.session = None
        self.closed = False
        self.wake_task = None
        self.owned_once = False
        self.handover_done = False
        self.claim_wake = claim_wake
        self.factory = factory
        self.strict_wake = strict_wake
        self.route = factory(self.send_control, self.send_data,
                             capabilities=tuple(device.capabilities),
                             private_state='local', exclusive=True)

    @property
    def active(self):
        return not self.closed and self.session is not None

    @property
    def requires_handover(self):
        return self.owned_once and not self.handover_done

    async def send_control(self, message):
        if self.device.control_ws is not self.control_ws:
            raise ConnectionError('Retired controller link')
        if message.get('type') not in {'rt_open','rt_flush','rt_close','listen_ack','listen_close'}:
            raise ValueError('Command outside realtime transport')
        async with asyncio.timeout(.5):
            # Native send_control catches errors; a receipt-driven route must
            # propagate them. Never substitute a replacement connection here.
            await self.control_ws.send(json.dumps(message))
        if message.get('type') == 'listen_close' and message.get('session') == self.session:
            self.session = None

    async def send_data(self, packet):
        if (self.closed or self.data_ws is None or self.device.data_ws is not self.data_ws
                or len(packet)>1957 or not packet or packet[0]!=9):
            raise ConnectionError('Retired data link or invalid realtime frame')
        async with asyncio.timeout(.5):
            await self.data_ws.send(packet)

    def available(self):
        return (self.device.secure and self.device.listen_view.private
                and not self.device.muted and self.device.wake_word_enabled
                and not self.device.voice_lock.locked()
                and not (self.device.private_turn_task is not None
                         and not self.device.private_turn_task.done())
                and not self.device.speaking and self.device.data_ws is not None)

    def eligible(self):
        return not self.closed and self.available()

    def replacement(self):
        # Rearm only after a verified native handover, once the native owner is
        # idle. Called by a new user wake or its private-session frame, never
        # by a timer, unmute, reconnection or a speaker receipt. No mic command.
        if (self.closed and self.handover_done and self.available()
                and not self.device.oww_paused.is_set()
                and self.device.control_ws is self.control_ws
                and self.device.data_ws is self.data_ws):
            binding = Binding(self.device, self.factory, self.claim_wake,
                              strict_wake=self.strict_wake)
            self.device.realtime = binding
            return binding
        return None

    async def _wake(self, event):
        try:
            arrived = asyncio.get_running_loop().time()
            async with asyncio.timeout(2):
                claimed = await self.claim_wake(self.device, event)
            event['ageMs'] += (asyncio.get_running_loop().time()-arrived)*1000
            if (not claimed or not self.eligible() or self.session != event['session']
                    or not await self.route.wake(event)):
                await self.send_control({'type':'listen_close','session':event['session'],
                                         'reason':'realtime_wake_rejected'})
        except (Exception, asyncio.CancelledError):
            await self.close()

    async def control(self, event):
        kind = event.get('type')
        if kind == 'oww_wake':
            replacement = self.replacement()
            if replacement is not None:
                return await replacement.control(event)
            if not self.eligible() or self.device.oww_paused.is_set():
                if self.strict_wake:
                    session = event.get('session')
                    if type(session) is int and 0 < session <= 0xffffffff:
                        try:
                            await self.send_control({'type':'listen_close',
                                'session':session,'reason':'candidate_not_ready'})
                        except Exception:
                            await self.close()
                    return True  # reject; never dispatch this wake to native HA
                return False  # native owner keeps its session
            session, age = event.get('session'), event.get('ageMs')
            if (type(session) is not int or not 0<session<=0xffffffff or
                    type(age) not in (int,float) or not math.isfinite(age) or not 0<=age<2500):
                return self.strict_wake
            if self.wake_task is not None and not self.wake_task.done():
                if session != self.session:
                    await self.send_control({'type':'listen_close','session':session,
                                             'reason':'realtime_busy'})
                return True  # never dispatch a second consumer for this wake
            self.session, self.data_ws = session, self.device.data_ws
            self.owned_once = True
            # Receipt/control reader must remain free while wake waits for the
            # physical rt_ready fence. Awaiting wake here deadlocks that reader.
            self.wake_task = asyncio.create_task(self._wake(dict(event)))
            return True
        if kind in {'rt_ready','rt_played','rt_done','rt_closed'}:
            if not self.closed or kind == 'rt_closed':
                await self.route.control(event)
            return True
        if kind in {'listen_end','mute_state'} and not self.closed:
            await self.route.control(event)
            if (kind=='listen_end' and event.get('session')==self.session) or event.get('muted') is True:
                self.session = None
        if kind=='listen_state' and event.get('state')!='local' and self.owned_once:
            # Startup negotiation reports unknown before local. Before any
            # claimed wake there is no speaker/session to retire. Keep the
            # unstarted binding gated by available(); do not permanently close
            # it. A state loss after ownership still closes the actual route.
            await self.close()
        if kind=='button' and self.active:
            await self.close()  # explicit button hands control back to native
        return False

    async def frame(self, packet):
        replacement = self.replacement()
        if replacement is not None:
            return await replacement.frame(packet)
        if not self.eligible() or self.device.oww_paused.is_set():
            return self.strict_wake  # bounded pilot drops rather than falls through
        if self.data_ws is None:
            self.data_ws = self.device.data_ws
        if self.device.data_ws is not self.data_ws:
            await self.close()
            return self.strict_wake
        # The route's existing small, timed pre-wake buffer resolves control/data
        # arrival order. It never records audio or opens a microphone itself.
        await self.route.frame(packet)
        return True

    async def handover(self):
        await self.close()
        async with asyncio.timeout(1):
            await self.route.wait_closed()
        self.handover_done = True

    async def close(self):
        if self.closed:
            return
        self.closed = True
        task = self.wake_task
        if task and task is not asyncio.current_task():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        try:
            async with asyncio.timeout(1):
                await self.route.close()
        except Exception:
            pass  # bounded teardown must never abort native controller cleanup
        finally:
            self.session = None
