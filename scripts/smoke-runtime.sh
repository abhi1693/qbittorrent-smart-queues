#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
image=${1:?Usage: smoke-runtime.sh IMAGE PLATFORM [APP_VERSION]}
platform=${2:?Usage: smoke-runtime.sh IMAGE PLATFORM [APP_VERSION]}
source_version=$(python3 scripts/check_version.py)
app_version=${3:-$source_version}
test "$app_version" = "$source_version"
case "$(uname -m):$platform" in
  x86_64:linux/amd64|aarch64:linux/arm64) ;;
  *) echo 'Runtime smoke requires a matching native runner' >&2; exit 1 ;;
esac
test "$(docker image inspect "$image" --format '{{.Os}}/{{.Architecture}}')" = "$platform"
test "$(docker image inspect "$image" --format '{{.Config.User}}')" = '65532:65532'
test "$(docker image inspect "$image" --format '{{json .Config.Entrypoint}}')" = '["python","-m","qbittorrent_smart_queues.guard"]'
test "$(docker image inspect "$image" --format '{{index .Config.Labels "org.opencontainers.image.version"}}')" = "$source_version"
python_version=$(sed -n 's/^FROM python:\([0-9.]*\)-.*$/\1/p' Dockerfile)
test -n "$python_version"
docker run --rm -i --platform "$platform" --network none --read-only \
  --cap-drop ALL --security-opt no-new-privileges --pids-limit 128 --memory 192m \
  --tmpfs /tmp:rw,noexec,nosuid,mode=1777,size=32m \
  -e EXPECTED_APP_VERSION="$app_version" -e EXPECTED_PYTHON_VERSION="$python_version" \
  -e EXPECTED_PLATFORM="$platform" --entrypoint python "$image" - < scripts/runtime_smoke.py
