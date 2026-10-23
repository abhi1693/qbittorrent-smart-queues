FROM python:3.14.9-alpine3.24@sha256:dbeb11d16bb95c3a7e3c25c842cb9f20cbc8c18f8be0461a4a0dcc861ad386a0

ARG APP_VERSION
ARG REVISION=unknown

LABEL org.opencontainers.image.source="https://github.com/abhi1693/qbittorrent-smart-queues" \
    org.opencontainers.image.version="${APP_VERSION}" \
    org.opencontainers.image.revision="${REVISION}"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app

# Both entrypoints use only the standard library; retain Python's native modules
# and timezone data, without shipping unused package installers.
RUN apk upgrade --no-cache \
    && python -m pip uninstall --yes pip \
    && rm -rf /usr/local/lib/python3.14/ensurepip

COPY --chown=65532:65532 src/ /app/src/
COPY --chown=65532:65532 pyproject.toml /app/pyproject.toml

RUN APP_VERSION="$APP_VERSION" python -c \
    'import os; from qbittorrent_smart_queues import __version__; assert os.environ["APP_VERSION"] == __version__'

USER 65532:65532
ENTRYPOINT ["python", "-m", "qbittorrent_smart_queues.guard"]
