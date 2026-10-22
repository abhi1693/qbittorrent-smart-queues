FROM python:3.14.9-alpine3.24@sha256:dbeb11d16bb95c3a7e3c25c842cb9f20cbc8c18f8be0461a4a0dcc861ad386a0

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY --chown=65532:65532 src/ /app/src/

USER 65532:65532
ENTRYPOINT ["python", "-m", "qbittorrent_smart_queues.guard"]
