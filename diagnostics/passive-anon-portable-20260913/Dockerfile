# Build from a caller-supplied, verified Python 3.11 image digest.
# Context must contain canonical source in source/ and this directory in harness/.
ARG PYTHON_IMAGE
FROM ${PYTHON_IMAGE}
WORKDIR /app
COPY source/requirements.lock /app/requirements.lock
RUN python -m pip install --no-cache-dir -r /app/requirements.lock
COPY source/ /app/
RUN python -m pip install --no-cache-dir --no-deps -e .
COPY harness/gate.py /diagnostics/gate.py
ENV PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "/diagnostics/gate.py"]
