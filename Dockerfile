FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PORT=8080 \
    PYTHONPATH=/app/src

WORKDIR /app

# Java is required for local Spark functionality from the dashboard.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY . .

EXPOSE 8080

CMD ["streamlit", "run", "dashboard.py", "--server.port=8080", "--server.address=0.0.0.0", "--server.headless=true", "--browser.gatherUsageStats=false"]
