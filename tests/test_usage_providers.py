import importlib
import unittest
from datetime import datetime, timezone
from unittest import mock

from qbittorrent_smart_queues.errors import ApiError
from qbittorrent_smart_queues.providers import (
    BackupInternetProvider,
    NetworkUsageProvider,
    ProviderDiagnostics,
    UsageProviderRegistry,
    UsageSnapshot,
)
from qbittorrent_smart_queues.providers.unifi import UnifiProvider


class ExampleProvider(NetworkUsageProvider, BackupInternetProvider):
    provider_name = "example"

    def usage_snapshot(self, now):
        return UsageSnapshot(100, 10)

    def backup_internet_state(self):
        return {"enabled": False}

    def diagnostics(self):
        return ProviderDiagnostics(usage_scope="example")


class UsageProviderTests(unittest.TestCase):
    def setUp(self):
        self.guard = importlib.import_module("qbittorrent_smart_queues.guard")

    def test_registry_creates_a_registered_provider(self):
        registry = UsageProviderRegistry()
        registry.register("example", ExampleProvider)

        provider = registry.create("EXAMPLE")

        self.assertIsInstance(provider, ExampleProvider)
        self.assertEqual(("example",), registry.available())

    def test_registry_rejects_unknown_provider(self):
        registry = UsageProviderRegistry()
        registry.register("example", ExampleProvider)

        with self.assertRaisesRegex(ApiError, "available: example"):
            registry.create("missing")

    def test_registry_rejects_non_subclass_even_if_methods_match(self):
        class DuckTypedProvider:
            provider_name = "duck"

            def usage_snapshot(self, now):
                return UsageSnapshot(0, 0)

        registry = UsageProviderRegistry()
        registry.register("duck", DuckTypedProvider)

        with self.assertRaisesRegex(TypeError, "incompatible object"):
            registry.create("duck")

    def test_registry_rejects_mismatched_provider_identity(self):
        registry = UsageProviderRegistry()
        registry.register("wrong-name", ExampleProvider)

        with self.assertRaisesRegex(TypeError, "returned provider 'example'"):
            registry.create("wrong-name")

    def test_base_summary_uses_typed_diagnostics(self):
        summary = ExampleProvider().decision_summary(
            datetime(2026, 8, 27, tzinfo=timezone.utc)
        )

        self.assertEqual("example", summary["provider"])
        self.assertEqual("example", summary["usage_scope"])

    def test_snapshot_rejects_negative_usage(self):
        with self.assertRaisesRegex(ValueError, "cannot be negative"):
            UsageSnapshot(-1, 0)

    def test_missing_backup_capability_has_clear_error(self):
        class UsageOnlyProvider(NetworkUsageProvider):
            provider_name = "usage-only"

            def usage_snapshot(self, now):
                return UsageSnapshot(0, 0)

        with self.assertRaisesRegex(ApiError, "does not support backup-internet"):
            self.guard.read_backup_internet_state(UsageOnlyProvider())

    def test_unifi_is_the_default_provider(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            provider = self.guard.usage_provider_from_env()

        self.assertIsInstance(provider, UnifiProvider)
        self.assertIsInstance(provider, NetworkUsageProvider)
        self.assertIsInstance(provider, BackupInternetProvider)

    def test_provider_can_be_explicitly_disabled(self):
        with mock.patch.dict(
            "os.environ",
            {"QBT_USAGE_PROVIDER": "none"},
            clear=True,
        ):
            self.assertIsNone(self.guard.usage_provider_from_env())

    def test_unknown_provider_fails_before_runtime_guards(self):
        with mock.patch.dict(
            "os.environ",
            {"QBT_USAGE_PROVIDER": "missing"},
            clear=True,
        ), mock.patch.object(self.guard, "apply_rpi_thermal_cooling") as cooling:
            result = self.guard.SmartQueueController().run_once()

        self.assertEqual(1, result)
        cooling.assert_not_called()

    def test_unifi_provider_returns_typed_snapshot(self):
        provider = UnifiProvider()
        provider.authenticated = True
        provider.stats_timezone_info = timezone.utc
        now = datetime(2026, 8, 27, tzinfo=timezone.utc)
        with mock.patch.dict(
            "os.environ",
            {
                "UNIFI_STATS_TIMEZONE": "UTC",
                "UNIFI_STATS_MAX_DOWNLOAD_RATE_BYTES_PER_SEC": "1",
            },
            clear=True,
        ), mock.patch.object(provider, "stats_rows", return_value=[]):
            snapshot = provider.usage_snapshot(now)

        self.assertIsInstance(snapshot, UsageSnapshot)
        self.assertEqual(0, snapshot.cycle_usage_bytes)
        self.assertEqual(0, snapshot.day_usage_bytes)
