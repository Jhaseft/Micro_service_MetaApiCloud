# eas-worker

Bot de trading que opera las cuentas de los clientes en **MetaApi Cloud** según
los bots y estrategias configurados en el panel **EasDashboard**. Proyecto
independiente: se despliega y escala por separado del panel.

No necesita MetaTrader instalado — cada cuenta corre en la nube de MetaApi.

## Cómo encaja en el sistema

```
[Panel EasDashboard]  ←──HTTPS + X-API-Key──→  [eas-worker (este proyecto)]
  cliente configura bots                          lee bots/estrategias
  guarda en su BD                                 y opera por MetaApi Cloud
```

- El **panel** es el cerebro de configuración: el cliente elige estrategia,
  símbolos, lotaje, SL/TP, trailing, etc. Todo se guarda en su base de datos.
- **Este worker** pregunta cada `POLL_SECONDS` a `GET /api/worker/accounts`,
  recibe las cuentas operables + bots activos con su estrategia y parámetros,
  baja velas de MetaApi, **corre la estrategia aquí** y manda las órdenes.
- La estrategia (`strategy_multitf.py`) vive en este proyecto. MetaApi solo
  ejecuta la orden final; la lógica nunca sale de aquí.

## Qué hace en cada ciclo

Por cada cuenta y cada bot activo:
1. **Trailing stop** de las posiciones abiertas (mueve el SL a favor, siempre).
2. Si está dentro del horario, **evalúa la estrategia** y abre operación si hay señal.

Estrategias soportadas:
- `simple` — abre según la dirección fija configurada.
- `multitf_orderflow` — tendencia EMA alineada en D1/H8/H4 + order flow en M5.
  Si su parámetro `live_mode` está desactivado, corre en **modo paper** (loguea,
  no envía) — ideal para validar en demo.

## Configuración

Copia `.env.example` a `.env` y complétalo:

| Variable | Qué es |
|---|---|
| `DASHBOARD_URL` | URL del panel (ej. `https://tu-panel.com`). Usa HTTPS en producción. |
| `BOT_API_KEY` | La misma clave que `BOT_API_KEY` en el `.env` del panel. |
| `POLL_SECONDS` | Cada cuántos segundos consulta el panel (ej. `30`). |

## Correr en local

```bash
pip install -r requirements.txt
cp .env.example .env        # y edítalo
python metaapi_worker.py
```

## Probar una orden suelta (sin panel)

```bash
# Requiere METAAPI_TOKEN y METAAPI_ACCOUNT_ID en el entorno o .env
python open_one.py EURUSD buy 0.01
```

## Deploy con Docker

```bash
docker build -t eas-worker .
docker run -d --name eas-worker --restart unless-stopped \
  -e DASHBOARD_URL="https://tu-panel.com" \
  -e BOT_API_KEY="la-misma-clave-del-panel" \
  -e POLL_SECONDS="30" \
  eas-worker
```

En un hosting tipo Coolify/Railway/Render: crea un servicio apuntando a este
repo (Dockerfile incluido) y define esas 3 variables de entorno.

> **Importante:** corre **una sola instancia** del worker. Si levantas varias,
> abrirían órdenes duplicadas (un worker ya maneja todas las cuentas de todos
> los clientes).

## Subir a git

```bash
git init
git add .
git commit -m "eas-worker: bot de trading MetaApi Cloud"
git branch -M main
git remote add origin <URL-de-tu-repo>
git push -u origin main
```

El `.env` real está en `.gitignore`; nunca se sube.
```
