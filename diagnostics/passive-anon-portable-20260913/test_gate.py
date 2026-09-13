"""Unit checks of the isolation refusal logic; not an isolation/reproduction proof."""
import os, tempfile, unittest
from pathlib import Path
from gate import verify

class GateTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name);self.cg=root/'cgroup';self.cg.mkdir();self.state=root/'state';self.state.mkdir()
        for name,value in {'cgroup.controllers':'memory pids','memory.max':'2147483648',
            'memory.current':'12345678','memory.stat':'anon 12000000\nfile 345678\n',
            'cgroup.procs':str(os.getpid())}.items():(self.cg/name).write_text(value)
    def test_accepts_contract_fixture(self):
        self.assertEqual(verify(self.cg,self.state)['memory_max'],2147483648)
    def test_rejects_shared_14g_limit(self):
        (self.cg/'memory.max').write_text('15032385536')
        with self.assertRaisesRegex(RuntimeError,'Expected exclusive 2 GiB'):verify(self.cg,self.state)
    def test_rejects_extra_member(self):
        (self.cg/'cgroup.procs').write_text(f'{os.getpid()}\n999999')
        with self.assertRaisesRegex(RuntimeError,'membership not proven'):verify(self.cg,self.state)
    def test_rejects_invisible_member(self):
        (self.cg/'cgroup.procs').write_text(f'{os.getpid()}\n0')
        with self.assertRaisesRegex(RuntimeError,'membership not proven'):verify(self.cg,self.state)
    def test_rejects_state_loaded_before_isolation(self):
        (self.state/'state.sqlite3').write_bytes(b'fixture')
        with self.assertRaisesRegex(RuntimeError,'initially be empty'):verify(self.cg,self.state)
    def test_rejects_child_cgroup(self):
        (self.cg/'unexpected').mkdir()
        with self.assertRaisesRegex(RuntimeError,'child cgroups'):verify(self.cg,self.state)

if __name__=='__main__':unittest.main()
