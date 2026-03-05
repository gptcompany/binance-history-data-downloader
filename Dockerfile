FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    TZ=UTC

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy source and scripts
COPY src /app/src
COPY config /app/config
COPY scripts /app/scripts

RUN chmod +x /app/scripts/run-sync.sh && \
    mkdir -p /app/data

RUN useradd --create-home --uid 1000 appuser && chown -R appuser:appuser /app
USER appuser

CMD ["bash", "scripts/run-sync.sh"]
