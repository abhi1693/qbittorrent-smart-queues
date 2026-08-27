"""Network usage provider contracts and registry."""

from qbittorrent_smart_queues.providers.base import (
    BackupInternetProvider,
    NetworkUsageProvider,
    ProviderDiagnostics,
    UsageProviderRegistry,
    UsageSnapshot,
)

usage_provider_registry = UsageProviderRegistry()


def register_usage_provider(name, factory, *, aliases=()):
    """Register a provider factory for application startup or tests."""

    usage_provider_registry.register(name, factory, aliases=aliases)


def register_builtin_usage_providers(*, debug=None, warning=None):
    """Install first-party adapters without leaking them into queue policy."""

    from qbittorrent_smart_queues.providers.unifi import UnifiProvider

    register_usage_provider(
        UnifiProvider.provider_name,
        lambda: UnifiProvider(debug=debug, warning=warning),
    )


__all__ = [
    "BackupInternetProvider",
    "NetworkUsageProvider",
    "ProviderDiagnostics",
    "UsageProviderRegistry",
    "UsageSnapshot",
    "register_builtin_usage_providers",
    "register_usage_provider",
    "usage_provider_registry",
]
