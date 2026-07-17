FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /usr/app

RUN apt-get update \
    && apt-get install --yes --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --requirement requirements.txt

RUN groupadd --gid 10001 dbt \
    && useradd --uid 10001 --gid dbt --no-create-home dbt \
    && chown -R dbt:dbt /usr/app

USER 10001:10001

ENTRYPOINT ["dbt"]
CMD ["--version"]
