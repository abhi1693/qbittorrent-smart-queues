import importlib
import unittest
from unittest import mock


class GenericDefaultsTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_qbt_urls_default_to_empty_list(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            self.assertEqual([], self.guard.qbt_urls())

    def test_udm_login_requires_configured_url(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(self.guard.ApiError, "UDM_URL"):
                self.guard.UdmClient().login()

    def test_nvme_thermal_guard_is_disabled_without_prometheus_url(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            state = self.guard.NvmeThermalGuard().check()

        self.assertFalse(state["enabled"])
        self.assertFalse(state["stop"])
        self.assertEqual("NVMe thermal guard disabled", state["reason"])

    def test_enabled_nvme_thermal_guard_requires_prometheus_url(self):
        with mock.patch.dict("os.environ", {"QBT_NVME_THERMAL_STOP_ENABLED": "true"}, clear=True):
            state = self.guard.NvmeThermalGuard().check()

        self.assertTrue(state["enabled"])
        self.assertTrue(state["stop"])
        self.assertIn("PROMETHEUS_URL", state["reason"])

    def test_optional_media_integrations_require_api_key_and_url(self):
        env = {
            "SONARR_API_KEY": "sonarr-key",
            "RADARR_API_KEY": "radarr-key",
            "JELLYFIN_API_KEY": "jellyfin-key",
        }

        with mock.patch.dict("os.environ", env, clear=True):
            sonarr = self.guard.SonarrQueueMetadata.__new__(self.guard.SonarrQueueMetadata)
            radarr = self.guard.RadarrQueueMetadata.__new__(self.guard.RadarrQueueMetadata)
            jellyfin = self.guard.JellyfinWatchMetadata.__new__(self.guard.JellyfinWatchMetadata)

            self.assertEqual([], sonarr.configs())
            self.assertEqual([], radarr.configs())
            self.assertEqual([], jellyfin.configs())

    def test_optional_media_integration_urls_are_used_when_configured(self):
        env = {
            "SONARR_API_KEY": "sonarr-key",
            "SONARR_URL": "http://sonarr.example/",
            "RADARR_API_KEY": "radarr-key",
            "RADARR_URL": "http://radarr.example/",
            "JELLYFIN_API_KEY": "jellyfin-key",
            "JELLYFIN_URL": "http://jellyfin.example/",
        }

        with mock.patch.dict("os.environ", env, clear=True):
            sonarr = self.guard.SonarrQueueMetadata.__new__(self.guard.SonarrQueueMetadata)
            radarr = self.guard.RadarrQueueMetadata.__new__(self.guard.RadarrQueueMetadata)
            jellyfin = self.guard.JellyfinWatchMetadata.__new__(self.guard.JellyfinWatchMetadata)

            self.assertEqual([("sonarr", "http://sonarr.example", "sonarr-key")], sonarr.configs())
            self.assertEqual([("radarr", "http://radarr.example", "radarr-key")], radarr.configs())
            self.assertEqual([("jellyfin", "http://jellyfin.example", "jellyfin-key")], jellyfin.configs())


if __name__ == "__main__":
    unittest.main()
