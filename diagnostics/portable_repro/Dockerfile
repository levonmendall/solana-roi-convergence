FROM python:3.11.16-slim
ARG SOURCE_SHA
RUN test "$SOURCE_SHA" = "92c0c1620f78116e7ecbeade039e9aedaf3a51a9"
ENV PORTABLE_REPRO_SOURCE_SHA=$SOURCE_SHA
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY . /app
RUN pip install --no-cache-dir -e .
ENTRYPOINT ["/bin/bash", "/app/diagnostics/portable_repro/run.sh"]
