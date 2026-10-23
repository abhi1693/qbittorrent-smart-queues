import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "runtime_smoke", ROOT / "scripts/runtime_smoke.py"
)
SMOKE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(SMOKE)


class RuntimeSmokeTests(unittest.TestCase):
    def test_controller_entrypoint_against_loopback_qbittorrent_fixture(self):
        proof = SMOKE.exercise_controller()
        self.assertTrue(proof["health"])
        self.assertEqual(0, proof["torrent_mutations"])

    def test_reconciler_entrypoint_verifies_receipts_without_writes(self):
        proof = SMOKE.exercise_reconciler()
        self.assertTrue(proof["complete_receipts"])
        self.assertEqual(0, proof["database_mutations"])


if __name__ == "__main__":
    unittest.main()
