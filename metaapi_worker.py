"""
eas-worker — Worker de trading MetaApi Cloud.

Proyecto INDEPENDIENTE que opera las cuentas de los clientes en la nube de
MetaApi según los bots/estrategias configurados en el panel EasDashboard.
No necesita MetaTrader instalado: cada cuenta corre en el cloud de MetaApi.

Flujo (cada POLL_SECONDS):
  1. Pregunta al panel:  GET /api/worker/accounts  (cabecera X-API-Key)
     -> token de MetaApi + cuentas operables, y dentro de cada una los bots
        activos con su estrategia y parámetros.
  2. Por cada cuenta abre una conexión RPC a MetaApi y, según cada bot:
       - gestiona el trailing stop de las posiciones abiertas,
       - evalúa la estrategia y abre operaciones (respetando horario y máximos).
  3. Repite.

Este worker NO es una web, pero levanta un pequeño servidor de salud para que
el hosting (Render/Railway/Coolify) no devuelva 404 al entrar a la raíz: la
ruta `/` y `/health` responden el estado actual en JSON.

Config por variables de entorno (o archivo .env en esta carpeta):
    DASHBOARD_URL   URL pública del panel (ej. https://tu-panel.com)
    BOT_API_KEY     misma clave que BOT_API_KEY en el .env del panel
    POLL_SECONDS    cada cuántos segundos consulta el panel (ej. 30)
    PORT            puerto del servidor de salud (lo inyecta el hosting; def 8080)
"""

import asyncio
import json
import logging
import os
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
from metaapi_cloud_sdk import MetaApi

try:
    # Carga variables desde un archivo .env en esta carpeta (si existe).
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

import strategy_multitf

# --- Logging: a stdout, con hora, para que el hosting muestre TODO en sus logs.
# El nivel global queda en WARNING para silenciar el ruido del SDK de MetaApi
# (socket.io / engine.io / httpx), que ADEMÁS imprime el token JWT dentro de las
# URLs -> riesgo de seguridad. Solo nuestro logger 'eas-worker' habla en INFO.
logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
for _noisy in ("socketio", "engineio", "httpx", "httpcore", "metaapi", "websockets"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)
log = logging.getLogger("eas-worker")
log.setLevel(logging.INFO)

# Cuántas velas pedir por timeframe para evaluar la estrategia multi-TF.
CANDLES_D1 = 150
CANDLES_H4 = 300  # 300 H4 -> 150 H8 sintéticas (suficiente para EMA50)
CANDLES_M5 = 100

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000").rstrip("/")
API_KEY = os.getenv("BOT_API_KEY", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
HEALTH_PORT = int(os.getenv("PORT", "8080"))

ACCOUNTS_ENDPOINT = f"{DASHBOARD_URL}/api/worker/accounts"
HEADERS = {"X-API-Key": API_KEY}

# Estado compartido que expone el servidor de salud (se actualiza cada ciclo).
STATUS = {
    "service": "eas-worker",
    "ok": True,
    "started_at": None,
    "panel": DASHBOARD_URL,
    "poll_seconds": POLL_SECONDS,
    "cycles": 0,
    "last_poll": None,
    "accounts": 0,
    "last_error": None,
}


# --------------------------------------------------------------------------- #
#  Servidor de salud (arregla el 404 en la raíz)
# --------------------------------------------------------------------------- #
class _HealthHandler(BaseHTTPRequestHandler):
    """Responde el estado del worker en `/` y `/health` (JSON, 200)."""

    def do_GET(self):  # noqa: N802 (nombre lo fija la librería)
        body = json.dumps(STATUS, indent=2).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):  # silencia el log ruidoso por request
        log.debug("health %s", fmt % args)


def start_health_server():
    """Levanta el servidor de salud en un hilo de fondo (no bloquea el worker)."""
    server = HTTPServer(("0.0.0.0", HEALTH_PORT), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    log.info(
        "Servidor de salud escuchando en http://0.0.0.0:%s/ "
        "(la raíz ya responde estado, no 404).", HEALTH_PORT
    )


def fetch_accounts():
    """Devuelve (metaapi_token, [cuentas]) desde el panel."""
    resp = requests.get(ACCOUNTS_ENDPOINT, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("metaapi_token"), data.get("accounts", [])


def pips_to_price(spec, pips):
    """Convierte pips a distancia de precio segun los digitos del simbolo."""
    if pips is None:
        return 0.0
    digits = spec.get("digits", 5)
    point = 10 ** (-digits)
    factor = 10 if digits in (3, 5) else 1
    return float(pips) * point * factor


async def count_open_positions(connection, symbol, magic):
    positions = await connection.get_positions()
    return len([p for p in positions if p.get("symbol") == symbol and p.get("magic") == magic])


async def fetch_candles(account, symbol, timeframe, limit):
    """Velas históricas (más antigua -> más reciente) para la estrategia."""
    candles = await account.get_historical_candles(
        symbol=symbol, timeframe=timeframe, start_time=None, limit=limit
    )
    return list(reversed(candles or []))


async def resolve_direction(account, connection, bot, symbol):
    """
    Decide la dirección a operar según la estrategia del bot.
      - 'multitf_orderflow': evalúa tendencia multi-TF + order flow (parámetros
        del panel). Devuelve 'buy'/'sell' o None si no hay señal.
      - cualquier otra ('simple'): usa la dirección fija configurada.
    Respeta además la restricción de dirección del panel (buy/sell/both).
    """
    configured = bot["entry"]["direction"]  # 'buy' | 'sell' | 'both'
    strategy = bot.get("strategy", "simple")

    if strategy != "multitf_orderflow":
        return configured  # comportamiento simple original

    params = bot.get("parameters", {}) or {}
    try:
        d1 = await fetch_candles(account, symbol, "1d", CANDLES_D1)
        h4 = await fetch_candles(account, symbol, "4h", CANDLES_H4)
        m5 = await fetch_candles(account, symbol, "5m", CANDLES_M5)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] %s: no se pudieron leer velas (%s).", bot["name"], symbol, exc)
        return None

    direction, reason = strategy_multitf.decide(d1, h4, m5, params)
    if direction is None:
        log.info("[%s] %s: sin señal (%s).", bot["name"], symbol, reason)
        return None

    # El panel puede restringir a solo compra o solo venta.
    if configured in ("buy", "sell") and configured != direction:
        log.info("[%s] %s: señal %s ignorada (el bot está fijado a %s).",
                 bot["name"], symbol, direction, configured)
        return None

    log.info("[%s] %s: señal %s (%s).", bot["name"], symbol, direction, reason)
    return direction


async def open_operation(account, connection, bot, symbol):
    """Abre una operacion para un bot+simbolo via MetaApi."""
    entry = bot["entry"]
    magic = 1000000 + int(bot["id"])

    if await count_open_positions(connection, symbol, magic) >= entry["max_open_trades"]:
        log.info("[%s] %s: ya alcanzo max_open_trades.", bot["name"], symbol)
        return

    direction = await resolve_direction(account, connection, bot, symbol)
    if direction is None:
        return  # la estrategia decidió no operar este ciclo

    try:
        price = await connection.get_symbol_price(symbol)
        spec = await connection.get_symbol_specification(symbol)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] %s: sin precio/spec (%s).", bot["name"], symbol, exc)
        return

    sl_dist = pips_to_price(spec, entry["stop_loss_pips"])
    tp_dist = pips_to_price(spec, entry["take_profit_pips"])
    volume = float(entry["lot_size"])
    options = {"comment": f"Eas:{bot['id']}", "magic": magic}

    # Modo paper: si el parámetro live_mode está desactivado, solo se loguea
    # la operación que se "habría" abierto (útil para validar en demo).
    params = bot.get("parameters", {}) or {}
    live_mode = bool(params.get("live_mode", True))
    if not live_mode:
        ref = price["bid"] if direction == "sell" else price["ask"]
        log.info("[%s] %s: [PAPER] habría abierto %s %s lotes @ %s (live_mode desactivado).",
                 bot["name"], symbol, direction, volume, ref)
        return

    if direction == "sell":
        ref = price["bid"]
        sl = ref + sl_dist if sl_dist else None
        tp = ref - tp_dist if tp_dist else None
        result = await connection.create_market_sell_order(symbol, volume, sl, tp, options)
    else:  # buy o both -> compra por defecto
        ref = price["ask"]
        sl = ref - sl_dist if sl_dist else None
        tp = ref + tp_dist if tp_dist else None
        result = await connection.create_market_buy_order(symbol, volume, sl, tp, options)

    log.info("[%s] %s: %s -> %s", bot["name"], symbol, direction, result.get("stringCode", result))


async def manage_trailing_stops(connection, bot, symbol):
    """
    Trailing stop "del lado del worker": mueve el SL de cada posición abierta del
    bot a medida que el precio avanza a favor. MetaApi/el broker NO lo hacen solos,
    por eso lo gestionamos aquí en cada ciclo.

    Regla: el SL solo se mueve para PROTEGER más (acercarse al precio), nunca para
    aflojarse. La distancia es `trailing_stop_pips` del panel.
    """
    trailing_pips = bot.get("risk", {}).get("trailing_stop_pips")
    if not trailing_pips:
        return  # trailing desactivado para este bot

    magic = 1000000 + int(bot["id"])
    positions = await connection.get_positions()
    mine = [p for p in positions if p.get("symbol") == symbol and p.get("magic") == magic]
    if not mine:
        return

    try:
        price = await connection.get_symbol_price(symbol)
        spec = await connection.get_symbol_specification(symbol)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] %s: trailing sin precio/spec (%s).", bot["name"], symbol, exc)
        return

    trail_dist = pips_to_price(spec, trailing_pips)
    if not trail_dist:
        return

    for pos in mine:
        pid = pos.get("id")
        ptype = pos.get("type")           # POSITION_TYPE_BUY / POSITION_TYPE_SELL
        cur_sl = pos.get("stopLoss")      # puede ser None
        cur_tp = pos.get("takeProfit")    # se conserva tal cual

        if ptype == "POSITION_TYPE_BUY":
            new_sl = price["bid"] - trail_dist
            improves = cur_sl is None or new_sl > cur_sl
        elif ptype == "POSITION_TYPE_SELL":
            new_sl = price["ask"] + trail_dist
            improves = cur_sl is None or new_sl < cur_sl
        else:
            continue

        if not improves:
            continue

        try:
            await connection.modify_position(pid, new_sl, cur_tp)
            log.info("[%s] %s: trailing -> SL movido a %s (pos %s).",
                     bot["name"], symbol, round(new_sl, 6), pid)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] %s: no se pudo mover SL de %s (%s).", bot["name"], symbol, pid, exc)


async def process_account(api, account_data):
    """Conecta una cuenta MetaApi y procesa sus bots activos."""
    account_id = account_data["metaapi_account_id"]
    bots = account_data.get("bots", [])
    if not bots:
        log.info("Cuenta %s: sin bots activos, se omite.", account_id)
        return

    log.info("Cuenta %s: procesando %s bot(s)...", account_id, len(bots))
    account = await api.metatrader_account_api.get_account(account_id)
    connection = account.get_rpc_connection()
    await connection.connect()
    try:
        await connection.wait_synchronized(60)

        for bot in bots:
            # 1) Trailing stop: protege posiciones abiertas SIEMPRE (incluso fuera
            #    de horario), porque ya hay dinero en riesgo.
            for symbol in bot.get("symbols", []):
                await manage_trailing_stops(connection, bot, symbol)

            # 2) Apertura de nuevas operaciones: solo dentro del horario.
            if not bot.get("within_trading_window", True):
                log.info("[%s] fuera de horario, no abre nuevas.", bot["name"])
                continue
            for symbol in bot.get("symbols", []):
                await open_operation(account, connection, bot, symbol)
    finally:
        await connection.close()


async def loop():
    while True:
        cycle_start = time.time()
        STATUS["cycles"] += 1
        cycle = STATUS["cycles"]
        log.info("Ciclo #%s: consultando panel %s ...", cycle, ACCOUNTS_ENDPOINT)
        try:
            token, accounts = fetch_accounts()
            STATUS["last_poll"] = datetime.now(timezone.utc).isoformat()
            STATUS["accounts"] = len(accounts)
            if not token:
                log.warning("El panel no devolvió METAAPI_TOKEN. Configuralo en el .env del panel.")
            else:
                api = MetaApi(token)
                log.info("Cuentas operables: %s", len(accounts))
                for account_data in accounts:
                    try:
                        await process_account(api, account_data)
                    except Exception as exc:  # noqa: BLE001
                        STATUS["last_error"] = str(exc)
                        log.exception("Cuenta %s: error", account_data.get("metaapi_account_id"))
            STATUS["ok"] = True
        except Exception as exc:  # noqa: BLE001
            STATUS["ok"] = False
            STATUS["last_error"] = str(exc)
            log.exception("Error en el ciclo")

        elapsed = time.time() - cycle_start
        log.info("Ciclo #%s terminado en %.1fs. Próxima consulta en %ss.",
                 cycle, elapsed, POLL_SECONDS)
        await asyncio.sleep(POLL_SECONDS)


if __name__ == "__main__":
    # Arranca el servidor de salud SIEMPRE (aunque falte config) para que el
    # hosting vea el puerto vivo y la raíz no dé 404.
    STATUS["started_at"] = datetime.now(timezone.utc).isoformat()
    start_health_server()

    if not API_KEY:
        STATUS["ok"] = False
        STATUS["last_error"] = "Falta BOT_API_KEY"
        log.error("Define BOT_API_KEY en el entorno o en el archivo .env.")
        raise SystemExit(1)

    log.info("eas-worker iniciado | panel=%s | cada %ss | health en :%s",
             DASHBOARD_URL, POLL_SECONDS, HEALTH_PORT)
    try:
        asyncio.run(loop())
    except KeyboardInterrupt:
        # El SDK de MetaApi deja hilos de fondo (no daemon) que impiden que el
        # proceso muera con un Ctrl+C normal. Forzamos la salida inmediata.
        log.info("Detenido por el usuario (Ctrl+C). Cerrando worker.")
        os._exit(0)
