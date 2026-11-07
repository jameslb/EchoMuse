"""Pure metadata parsing: no optional websocket runtime, devices or audio."""
import json
from pathlib import Path
import sys
import unittest

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from em_realtime_bootstrap import validate_aec_readiness


def readback(capability=3, scope='one-dot-aec-fullprecision'):
    permit={'approved':True,'scope':scope,'expires_at':1100,'maximum_seconds':20}
    state={'version':1,'pid':123,'start_ticks':'456','available':True}
    if capability is not None:state['full_precision_version']=capability
    fields=['0']*22;fields[19]='456'
    return '\n'.join(['700','600','1000','permit.local.json',
        '"wakeWordEnabled":true','"wakeWordEnabled":true',json.dumps(state),
        '123 (server) '+' '.join(fields),json.dumps(permit)])


class PrecisionReadiness(unittest.TestCase):
    def test_reference_integrity_is_distinct_from_precision(self):
        raw=readback(scope='one-dot-aec-reference-integrity')
        self.assertFalse(validate_aec_readiness(raw)['ready'])
        lines=raw.splitlines();state=json.loads(lines[6]);state['reference_integrity_version']=4
        lines[6]=json.dumps(state)
        result=validate_aec_readiness('\n'.join(lines))
        self.assertTrue(result['ready'])
        self.assertEqual(result['maximum_loopback_bytes'],2_560_000)
        self.assertEqual(result['maximum_microphone_bytes'],8_960_000)
        self.assertEqual(validate_aec_readiness(readback())['maximum_loopback_bytes'],0)

    def test_new_scope_requires_capability_and_reports_exact_bounds(self):
        result=validate_aec_readiness(readback())
        self.assertTrue(result['ready'])
        self.assertEqual(result['scope'],'one-dot-aec-fullprecision')
        self.assertEqual(result['maximum_microphone_bytes'],8_960_000)
        for version in (None,2,True):
            with self.subTest(version=version):
                self.assertFalse(validate_aec_readiness(readback(version))['ready'])

    def test_old_scope_cannot_claim_raw_channels_and_unknown_scope_fails(self):
        old=validate_aec_readiness(readback(None,'one-dot-aec-uplink'))
        self.assertTrue(old['ready'])
        self.assertEqual(old['maximum_microphone_bytes'],0)
        self.assertFalse(validate_aec_readiness(readback(scope='other'))['ready'])


if __name__=='__main__':unittest.main()
