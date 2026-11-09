FROM python:3.12-slim
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1
RUN apt-get update \
    && apt-get install -y --no-install-recommends fonts-dejavu-core tzdata \
    && rm -rf /var/lib/apt/lists/*
COPY services/requirements*.txt services/
RUN python -m pip install --no-cache-dir -r services/requirements.txt
COPY services/ services/
COPY api/ api/
COPY sql/001_initial_schema.sql sql/
