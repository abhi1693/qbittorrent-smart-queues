import importlib.util
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location(
    "check_version", ROOT / "scripts/check_version.py"
)
VERSION = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(VERSION)


class ReleaseVersionTests(unittest.TestCase):
    def test_branch_and_matching_tag_use_the_dynamic_source_version(self):
        version = VERSION.source_version(ROOT)
        self.assertEqual(version, VERSION.check_version(version, {}))
        self.assertEqual(
            version,
            VERSION.check_version(
                version,
                {
                    "GITHUB_REF_TYPE": "tag",
                    "GITHUB_REF_NAME": f"v{version}",
                    "RELEASE_VERSION": version,
                },
            ),
        )

    def test_manual_and_tag_mismatches_fail_before_any_image_build(self):
        version = VERSION.source_version(ROOT)
        for environment in [
            {"RELEASE_VERSION": "9.9.9"},
            {"GITHUB_REF_TYPE": "tag", "GITHUB_REF_NAME": "v9.9.9"},
        ]:
            with self.subTest(environment=environment), self.assertRaises(ValueError):
                VERSION.check_version(version, environment)


if __name__ == "__main__":
    unittest.main()
