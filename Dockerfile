FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.runtime.txt ./
RUN pip install --no-cache-dir -r requirements.runtime.txt

COPY . .

RUN mkdir -p /app/.cache /app/charts

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=10s --retries=3 --start-period=25s \
  CMD python scripts/healthcheck.py

CMD ["python", "bot.py"]
