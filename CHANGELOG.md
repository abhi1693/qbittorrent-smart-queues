# Changelog

## 0.2.14 — 2026-11-13

- Make availability admission non-destructive: incomplete swarms are deferred without deleting or blocklisting downloaded data.
- Preserve manual force-start overrides and completed downloads throughout availability sampling.
- Preserve unknown-manager torrents and downloaded payload during metadata-timeout handling.

## 0.2.13 — 2026-10-23

- Update the Python runtime to 3.14.9 on Alpine 3.24 with current operating-system and TLS trust data.
- Remove unused runtime package installers while retaining native SQLite, compression and timezone support for the controller and Ryokan receipt adapter.
- Raise the build-tool floor to setuptools 84.2.1; the application runtime remains standard-library only.
