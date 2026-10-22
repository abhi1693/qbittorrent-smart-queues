FROM python:3.14.8-alpine3.24@sha256:f6a589d43c42b9e7f7dc67a12d37132491f362859a5d750607710cc56da3bc72

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY --chown=65532:65532 src/ /app/src/

USER 65532:65532
ENTRYPOINT ["python", "-m", "qbittorrent_smart_queues.guard"]
