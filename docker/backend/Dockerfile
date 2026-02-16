FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN apt-get update && apt-get install -y --no-install-recommends \
	wget ca-certificates gnupg2 fonts-liberation libnss3 libatk1.0-0 libatk-bridge2.0-0 libx11-xcb1 \
	libxcomposite1 libxdamage1 libxrandr2 libgbm1 libasound2 libpangocairo-1.0-0 libxss1 libxshmfence1 \
	chromium \
	&& rm -rf /var/lib/apt/lists/* \
	&& pip install --no-cache-dir -r /app/requirements.txt
COPY . /app
ENV PYTHONUNBUFFERED=1
CMD ["uvicorn", "src.backend.app:app", "--host", "0.0.0.0", "--port", "8000"]
