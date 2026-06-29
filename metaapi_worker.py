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
import strategy_asian
import copy_trade

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

# Asian breakout: usa el timeframe del bot. Pedimos suficientes velas para cubrir
# el día actual (sesión asiática) + el lookback de volumen.
CANDLES_ASIAN = 400

# Mapa de timeframes del panel (M1..D1) a los de MetaApi (1m..1d).
METAAPI_TF = {
    "M1": "1m", "M5": "5m", "M10": "10m", "M15": "15m",
    "M30": "30m", "H1": "1h", "H4": "4h", "D1": "1d",
}

DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://127.0.0.1:8000").rstrip("/")
API_KEY = os.getenv("BOT_API_KEY", "")
POLL_SECONDS = int(os.getenv("POLL_SECONDS", "30"))
# Cada cuánto se REFRESCA la configuración de copy (quién sigue a quién). Las
# operaciones NO se pollean: llegan por eventos de streaming.
COPY_CONFIG_SECONDS = int(os.getenv("COPY_CONFIG_SECONDS", "60"))
HEALTH_PORT = int(os.getenv("PORT", "8080"))

ACCOUNTS_ENDPOINT = f"{DASHBOARD_URL}/api/worker/accounts"
COPY_ACCOUNTS_ENDPOINT = f"{DASHBOARD_URL}/api/worker/copy-accounts"
COPY_TRADES_ENDPOINT = f"{DASHBOARD_URL}/api/worker/copy-trades"
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
    "copy_cycles": 0,
    "copy_last_poll": None,
    "copy_masters": 0,
    "copy_last_error": None,
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


def fetch_copy_accounts():
    """Devuelve (metaapi_token, [maestras]) para el copy-trading."""
    resp = requests.get(COPY_ACCOUNTS_ENDPOINT, headers=HEADERS, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    return data.get("metaapi_token"), data.get("masters", [])


def report_copy_trades(opened, closed):
    """Reporta al panel las aperturas/cierres de copia ejecutadas este ciclo."""
    resp = requests.post(
        COPY_TRADES_ENDPOINT,
        headers=HEADERS,
        json={"opened": opened, "closed": closed},
        timeout=20,
    )
    resp.raise_for_status()


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


def _pip_size(spec):
    """Tamaño de un pip en precio según los dígitos del símbolo."""
    digits = spec.get("digits", 5)
    point = 10 ** (-digits)
    factor = 10 if digits in (3, 5) else 1
    return point * factor


def _min_stop_distance(spec):
    """
    Distancia mínima (en PRECIO) que el broker exige entre el precio actual y
    el SL/TP. Si SL/TP quedan más cerca que esto, MetaApi devuelve
    'Invalid stops in the request'. El campo viene en 'points' (stopsLevel).
    """
    stops_level = spec.get("stopsLevel") or spec.get("stops_level") or 0
    digits = spec.get("digits", 5)
    point = 10 ** (-digits)
    return float(stops_level) * point


def _enforce_min_stops(sell, ref, sl, tp, min_dist, digits, bot, symbol):
    """
    Aleja SL/TP hasta la distancia mínima del broker para evitar el rechazo
    'Invalid stops'. Avisa por log cuando tiene que corregir un nivel.
    """
    if not min_dist:
        return sl, tp
    if sell:
        if sl is not None and (sl - ref) < min_dist:
            sl = round(ref + min_dist, digits)
            log.warning("[%s] %s: SL muy ajustado, movido al mínimo del broker (%s).",
                        bot["name"], symbol, sl)
        if tp is not None and (ref - tp) < min_dist:
            tp = round(ref - min_dist, digits)
            log.warning("[%s] %s: TP muy ajustado, movido al mínimo del broker (%s).",
                        bot["name"], symbol, tp)
    else:
        if sl is not None and (ref - sl) < min_dist:
            sl = round(ref - min_dist, digits)
            log.warning("[%s] %s: SL muy ajustado, movido al mínimo del broker (%s).",
                        bot["name"], symbol, sl)
        if tp is not None and (tp - ref) < min_dist:
            tp = round(ref + min_dist, digits)
            log.warning("[%s] %s: TP muy ajustado, movido al mínimo del broker (%s).",
                        bot["name"], symbol, tp)
    return sl, tp


def _respects_configured(configured, direction):
    """El panel puede restringir el bot a solo compra o solo venta."""
    return not (configured in ("buy", "sell") and configured != direction)


async def resolve_direction(account, connection, bot, symbol, spec):
    """
    Decide la dirección a operar según la estrategia del bot. Devuelve una tupla
    (direction, levels):
      - direction: 'buy' / 'sell' / 'both' / None (None = no operar este ciclo).
      - levels: None, o un dict con niveles del rango (solo 'asian_breakout') para
        que open_operation calcule SL/TP basados en el rango en vez de pips fijos.

    Estrategias:
      - 'multitf_orderflow': tendencia multi-TF + order flow.
      - 'asian_breakout': ruptura del rango asiático en sesión Londres/NY.
      - cualquier otra ('simple'): usa la dirección fija configurada.
    Respeta además la restricción de dirección del panel (buy/sell/both).
    """
    configured = bot["entry"]["direction"]  # 'buy' | 'sell' | 'both'
    strategy = bot.get("strategy", "simple")
    params = bot.get("parameters", {}) or {}

    if strategy == "multitf_orderflow":
        try:
            d1 = await fetch_candles(account, symbol, "1d", CANDLES_D1)
            h4 = await fetch_candles(account, symbol, "4h", CANDLES_H4)
            m5 = await fetch_candles(account, symbol, "5m", CANDLES_M5)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] %s: no se pudieron leer velas (%s).", bot["name"], symbol, exc)
            return None, None

        direction, reason = strategy_multitf.decide(d1, h4, m5, params)
        if direction is None:
            log.info("[%s] %s: sin señal (%s).", bot["name"], symbol, reason)
            return None, None
        if not _respects_configured(configured, direction):
            log.info("[%s] %s: señal %s ignorada (el bot está fijado a %s).",
                     bot["name"], symbol, direction, configured)
            return None, None
        log.info("[%s] %s: señal %s (%s).", bot["name"], symbol, direction, reason)
        return direction, None

    if strategy == "asian_breakout":
        tf = METAAPI_TF.get(bot.get("timeframe", "M15"), "15m")
        try:
            candles = await fetch_candles(account, symbol, tf, CANDLES_ASIAN)
        except Exception as exc:  # noqa: BLE001
            log.warning("[%s] %s: no se pudieron leer velas %s (%s).", bot["name"], symbol, tf, exc)
            return None, None
        if not candles:
            log.info("[%s] %s: sin velas %s para evaluar.", bot["name"], symbol, tf)
            return None, None

        server_hour = strategy_asian.broker_hour(candles[-1])
        direction, reason, levels = strategy_asian.decide(
            candles, params, server_hour=server_hour, pip_size=_pip_size(spec)
        )
        if direction is None:
            log.info("[%s] %s: sin entrada asian (%s).", bot["name"], symbol, reason)
            return None, None
        if not _respects_configured(configured, direction):
            log.info("[%s] %s: %s ignorada (el bot está fijado a %s).",
                     bot["name"], symbol, direction, configured)
            return None, None
        log.info("[%s] %s: %s (%s).", bot["name"], symbol, direction, reason)
        return direction, levels

    # 'simple': dirección fija configurada.
    return configured, None


async def open_operation(account, connection, bot, symbol):
    """Abre una operacion para un bot+simbolo via MetaApi."""
    entry = bot["entry"]
    magic = 1000000 + int(bot["id"])

    if await count_open_positions(connection, symbol, magic) >= entry["max_open_trades"]:
        log.info("[%s] %s: ya alcanzo max_open_trades.", bot["name"], symbol)
        return

    try:
        price = await connection.get_symbol_price(symbol)
        spec = await connection.get_symbol_specification(symbol)
    except Exception as exc:  # noqa: BLE001
        log.warning("[%s] %s: sin precio/spec (%s).", bot["name"], symbol, exc)
        return

    direction, levels = await resolve_direction(account, connection, bot, symbol, spec)
    if direction is None:
        return  # la estrategia decidió no operar este ciclo

    digits = spec.get("digits", 5)
    volume = float(entry["lot_size"])
    options = {"comment": f"Eas:{bot['id']}", "magic": magic}

    # SL/TP: por rango (asian_breakout, cuando hay 'levels') o por pips fijos.
    sell = direction == "sell"
    ref = price["bid"] if sell else price["ask"]
    if levels:
        sl, tp = _levels_to_sl_tp(sell, ref, levels, digits)
    else:
        sl_dist = pips_to_price(spec, entry["stop_loss_pips"])
        tp_dist = pips_to_price(spec, entry["take_profit_pips"])
        if sell:
            sl = round(ref + sl_dist, digits) if sl_dist else None
            tp = round(ref - tp_dist, digits) if tp_dist else None
        else:
            sl = round(ref - sl_dist, digits) if sl_dist else None
            tp = round(ref + tp_dist, digits) if tp_dist else None

    # El broker rechaza ('Invalid stops') SL/TP más cerca del precio que su
    # distancia mínima. Los alejamos hasta ese mínimo antes de enviar.
    min_dist = _min_stop_distance(spec)
    sl, tp = _enforce_min_stops(sell, ref, sl, tp, min_dist, digits, bot, symbol)

    # Modo paper: si el parámetro live_mode está desactivado, solo se loguea
    # la operación que se "habría" abierto (útil para validar en demo).
    params = bot.get("parameters", {}) or {}
    live_mode = bool(params.get("live_mode", True))
    if not live_mode:
        log.info("[%s] %s: [PAPER] habría abierto %s %s lotes @ %s (SL %s / TP %s, "
                 "live_mode desactivado).", bot["name"], symbol, direction, volume, ref, sl, tp)
        return

    log.info("[%s] %s: enviando %s %s lotes @ %s (SL %s / TP %s, min_dist %s)",
             bot["name"], symbol, direction, volume, ref, sl, tp, min_dist)

    if sell:
        result = await connection.create_market_sell_order(symbol, volume, sl, tp, options)
    else:  # buy o both -> compra por defecto
        result = await connection.create_market_buy_order(symbol, volume, sl, tp, options)

    log.info("[%s] %s: %s @ %s (SL %s / TP %s) -> %s",
             bot["name"], symbol, direction, ref, sl, tp, result.get("stringCode", result))


def _levels_to_sl_tp(sell, ref, levels, digits):
    """
    SL/TP del breakout asiático a partir del rango (replica el EA original):
      - SL al otro lado del rango +/- buffer, con un piso mínimo de ATR.
      - TP a una distancia = SL * ratio R:R.
    """
    atr_floor = levels.get("atr_floor", 0.0)
    rr = levels.get("rr", 2.0)
    if sell:
        sl = levels["range_high"] + levels["eff_buffer"]
        if atr_floor > 0 and (sl - ref) < atr_floor:
            sl = ref + atr_floor
        sl_dist = sl - ref
        tp = ref - sl_dist * rr
    else:
        sl = levels["range_low"] - levels["eff_buffer"]
        if atr_floor > 0 and (ref - sl) < atr_floor:
            sl = ref - atr_floor
        sl_dist = ref - sl
        tp = ref + sl_dist * rr
    return round(sl, digits), round(tp, digits)


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
        except requests.exceptions.HTTPError as exc:
            STATUS["ok"] = False
            status_code = exc.response.status_code if exc.response is not None else None
            STATUS["last_error"] = f"HTTP {status_code} en {ACCOUNTS_ENDPOINT}"
            if status_code in (401, 403):
                log.error(
                    "El panel rechazó la API key (HTTP %s). La BOT_API_KEY del worker "
                    "NO coincide con la del panel. Revisa que sean idénticas (sin "
                    "comillas ni espacios) y reinicia ambos servicios.", status_code
                )
            elif status_code == 503:
                log.error(
                    "El panel no tiene configurada BOT_API_KEY (HTTP 503). Define "
                    "BOT_API_KEY en las variables del panel y reinícialo."
                )
            else:
                log.error("El panel respondió HTTP %s al pedir las cuentas.", status_code)
        except Exception as exc:  # noqa: BLE001
            STATUS["ok"] = False
            STATUS["last_error"] = str(exc)
            log.exception("Error en el ciclo")

        elapsed = time.time() - cycle_start
        log.info("Ciclo #%s terminado en %.1fs. Próxima consulta en %ss.",
                 cycle, elapsed, POLL_SECONDS)
        await asyncio.sleep(POLL_SECONDS)


async def copy_loop():
    """
    Copy-trading por EVENTOS (streaming), sin polling de operaciones.

    Este bucle SOLO refresca la configuración (qué maestras y esclavas hay) cada
    COPY_CONFIG_SECONDS. Las aperturas/cierres reales las disparan los eventos de
    streaming dentro de CopyManager, no este bucle.
    """
    manager = None
    manager_token = None

    while True:
        STATUS["copy_cycles"] += 1
        try:
            token, masters = fetch_copy_accounts()
            STATUS["copy_last_poll"] = datetime.now(timezone.utc).isoformat()
            STATUS["copy_masters"] = len(masters)

            if not token:
                log.warning("Copy: el panel no devolvió METAAPI_TOKEN; no se copia.")
            else:
                if manager is None or token != manager_token:
                    manager = copy_trade.CopyManager(MetaApi(token), report_copy_trades)
                    manager_token = token
                await manager.sync_config(masters)

            STATUS["copy_last_error"] = None
        except requests.exceptions.HTTPError as exc:
            status_code = exc.response.status_code if exc.response is not None else None
            STATUS["copy_last_error"] = f"HTTP {status_code} en {COPY_ACCOUNTS_ENDPOINT}"
            log.error("Copy: el panel respondió HTTP %s al pedir las maestras.", status_code)
        except Exception as exc:  # noqa: BLE001
            STATUS["copy_last_error"] = str(exc)
            log.exception("Copy: error refrescando configuración")

        await asyncio.sleep(COPY_CONFIG_SECONDS)


async def main():
    """Corre el bucle de bots y el de copy-trading en paralelo."""
    await asyncio.gather(loop(), copy_loop())


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

    log.info("eas-worker iniciado | panel=%s | bots cada %ss | copy por eventos (config cada %ss) | health en :%s",
             DASHBOARD_URL, POLL_SECONDS, COPY_CONFIG_SECONDS, HEALTH_PORT)
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        # El SDK de MetaApi deja hilos de fondo (no daemon) que impiden que el
        # proceso muera con un Ctrl+C normal. Forzamos la salida inmediata.
        log.info("Detenido por el usuario (Ctrl+C). Cerrando worker.")
        os._exit(0)
