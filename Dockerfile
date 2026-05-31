FROM docker.io/library/python:3.13.11-alpine3.22@sha256:2fd93799bfc6381d078a8f656a5f45d6092e5d11d16f55889b3d5cbfdc64f045

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src

WORKDIR /app
COPY --chown=65532:65532 src/ /app/src/

USER 65532:65532
ENTRYPOINT ["python", "-m", "qbittorrent_smart_queues.guard"]
