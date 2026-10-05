# One image for the three Python services (PLC simulator, gateway, collector).
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY pyproject.toml LICENSE ./
COPY src ./src
RUN pip install . \
    && useradd --system --uid 10001 --no-create-home usine40 \
    && mkdir /data \
    && chown usine40 /data

USER usine40
ENV USINE40_HEALTH_FILE=/tmp/alive \
    USINE40_EVENT_LOG=/data/events.jsonl

HEALTHCHECK --interval=5s --timeout=3s --start-period=30s --retries=3 \
    CMD ["python", "-m", "usine40.health", "/tmp/alive"]
