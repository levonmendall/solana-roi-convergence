FROM python:3.11.16-slim
ARG SOURCE_SHA
RUN test "$SOURCE_SHA" = "92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
ENV PORTABLE_REPRO_SOURCE_SHA=$SOURCE_SHA
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
ENV PORTABLE_REPRO_NETWORK_AUDIT=/evidence/network-guard.jsonl
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates openssl procps iproute2 \
    && rm -rf /var/lib/apt/lists/*
COPY . /app
RUN test "$(cat /app/.canonical_source_sha)" = "$SOURCE_SHA" \
    && test -s /app/.harness_source_sha \
    && test -s /app/.canonical_source_tree_sha256
RUN pip install --no-cache-dir -e .
ENTRYPOINT ["/bin/bash", "/app/diagnostics/portable_repro/run.sh"]
