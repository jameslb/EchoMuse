import json
import os
from pathlib import Path
import tempfile
import unittest
import sys
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import em_realtime_bootstrap as bootstrap


class BootstrapTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.config={'enabled':True,'device_id':'synthetic-dot','pipeline_id':'synthetic-pipeline',
            'speech_host':'core-piper','speech_port':10200,'voice':'en_US-ryan-medium',
            'engineering_voice':True,'read_only_test_mode':True}

    def read(self,config):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'config.local.json'
            path.write_text(json.dumps(config),encoding='utf-8')
            return bootstrap.read_config(path)

    async def test_no_config_does_not_register_or_load_backend(self):
        with patch.dict(os.environ,{},clear=True):self.assertFalse(await bootstrap.start())

    def test_explicit_local_engineering_config(self):
        self.assertEqual(self.read(self.config),self.config)

    def test_no_internet_or_metadata_endpoint(self):
        for host in ['example.com','8.8.8.8','169.254.169.254','127.0.0.1','::1','224.0.0.1']:
            with self.assertRaises(ValueError):self.read({**self.config,'speech_host':host})

    def test_no_implicit_voice_substitution(self):
        with self.assertRaises(ValueError):self.read({**self.config,'engineering_voice':False})

    def test_no_unbounded_or_unknown_configuration(self):
        for change in [{'read_only_test_mode':False},{'enabled':1},{'extra':'value'},
                       {'speech_port':True},{'device_id':'*'}]:
            with self.assertRaises(ValueError):self.read({**self.config,**change})


if __name__=='__main__':unittest.main()
