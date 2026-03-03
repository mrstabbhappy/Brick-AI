FROM mcr.microsoft.com/devcontainers/python:3.12

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Ensure local upload path exists in the image
RUN mkdir -p /app/uploads

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    curl \
 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV PORT=80
EXPOSE 80

CMD ["sh", "-c", "gunicorn -b 0.0.0.0:${PORT:-80} --workers 2 --threads 4 app:app"]