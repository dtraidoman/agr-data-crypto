# The collector is deliberately portable: the same image runs on GitHub Actions
# and on a VPS / Oracle Cloud Always Free instance once Actions stops being a
# good fit. The only difference is COLLECTOR_SOFT_DEADLINE_SEC.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY src ./src
COPY scripts ./scripts

ENV PYTHONPATH=/app/src

# Defaults for a long-running deployment; Actions overrides the deadline.
ENV COLLECTOR_SOFT_DEADLINE_SEC=2147483647

ENTRYPOINT ["python", "-m", "collector.main"]
