FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
COPY objstore_cli.py /usr/local/bin/objstore_cli.py

WORKDIR /work
ENTRYPOINT ["python", "/usr/local/bin/objstore_cli.py"]
