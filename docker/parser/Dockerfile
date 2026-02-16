FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy application
COPY . /app

# ensure entrypoint is present
RUN chmod +x /app/docker/parser/entrypoint.sh || true

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["/app/docker/parser/entrypoint.sh"]
