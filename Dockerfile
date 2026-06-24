# eas-worker — imagen del bot de trading (Python + MetaApi Cloud).
# No sirve web; solo corre el bucle del worker.
FROM python:3.12-slim

# PYTHONUNBUFFERED: imprime los logs al instante (sin esto la salida se queda
# en buffer y los logs del hosting salen "en blanco").
ENV PYTHONUNBUFFERED=1 \
    PORT=8080

WORKDIR /app

# Dependencias primero (mejor cache de capas).
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código del worker (incluye strategy_multitf.py).
COPY . .

# Las variables (DASHBOARD_URL, BOT_API_KEY, POLL_SECONDS) se inyectan en el
# entorno del contenedor (en tu hosting o con `docker run -e ...`).

# Servidor de salud: la raíz `/` y `/health` responden el estado en JSON, así el
# hosting no devuelve 404 al entrar a la URL.
EXPOSE 8080

CMD ["python", "metaapi_worker.py"]
