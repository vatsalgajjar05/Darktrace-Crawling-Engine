FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONUTF8=1

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --gid 1000 appgroup \
    && useradd --uid 1000 --gid appgroup --create-home appuser

COPY . .

RUN mkdir -p crawl_output/raw /var/lib/celery \
    && chown -R appuser:appgroup /app /var/lib/celery

CMD ["python", "-c", "print('DarkTrace P2 container ready')"]
