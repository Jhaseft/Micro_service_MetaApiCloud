# eas-worker — imagen del bot de trading (Python + MetaApi Cloud).
# No sirve web; solo corre el bucle del worker.
FROM python:3.12-slim

WORKDIR /app

# Dependencias primero (mejor cache de capas).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código del worker (incluye strategy_multitf.py).
COPY . .

# Las variables (DASHBOARD_URL, BOT_API_KEY, POLL_SECONDS) se inyectan en el
# entorno del contenedor (en tu hosting o con `docker run -e ...`).
CMD ["python", "metaapi_worker.py"]
