"""
copy_trade.py — Copia automática maestra -> esclavas (open + close) en TIEMPO REAL.

Diseño (websocket permanente):
  - Por cada cuenta MAESTRA abrimos UNA conexión de STREAMING permanente a MetaApi.
    Esa conexión mantiene en memoria (terminal_state) las posiciones ABIERTAS de la
    maestra, actualizadas al instante por websocket. Ya NO hacemos polling por RPC de
    las posiciones: las leemos de terminal_state -> latencia de milisegundos y sin el
    "Timed out waiting for MetaApi to synchronize" del RPC que detenía la copia.
  - Un SynchronizationListener dispara una reconciliación (coalescida) en cada evento
    de posición de la maestra (abre/cierra) -> la copia reacciona casi al instante.
  - Además, un poll de respaldo (metaapi_worker.reconcile_loop, cada RECONCILE_SECONDS)
    reconcilia por si se perdiera algún evento; ahora es barato porque lee de memoria.
  - reconcile() abre en cada esclava lo que la maestra tiene y aún no se copió, y cierra
    lo que la maestra ya cerró. Las ÓRDENES en las esclavas se envían por RPC
    (ConnectionPool). Reporta al panel (POST /copy-trades).

Robustez (dinero real):
  - Solo se ACTÚA cuando la conexión de la maestra está SINCRONIZADA y su terminal
    conectado al bróker. Si no lo está, se OMITE el ciclo (no se abre ni se cierra
    nada): nunca cerramos por un estado desconocido o transitorio.
  - Si el streaming aún no sincroniza, se usa un fallback PUNTUAL por RPC para leer las
    posiciones (fuente completa y autoritativa) — así la copia no se detiene mientras
    el websocket termina de engancharse.
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from metaapi_cloud_sdk import SynchronizationListener

log = logging.getLogger("eas-worker")

# Cuánta historia carga el streaming al sincronizar. Para copiar solo hacen falta las
# posiciones ABIERTAS, no el historial; limitarlo evita que en cuentas con mucho
# historial la sincronización tarde o entre en bucle de resync.
STREAM_HISTORY_HOURS = 1
# Tiempo máximo que esperamos a que el streaming sincronice al conectar (no bloquea la
# copia: si tarda, seguimos y usamos el fallback por RPC hasta que sincronice).
STREAM_SYNC_TIMEOUT = 30


# --------------------------------------------------------------------------- #
#  Conexiones RPC a las esclavas (para EJECUTAR órdenes), cacheadas.
# --------------------------------------------------------------------------- #
class ConnectionPool:
    """Cachea conexiones RPC de MetaApi por cuenta para no reconectar cada vez."""

    def __init__(self, api):
        self.api = api
        self._conns = {}

    async def get(self, account_id):
        conn = self._conns.get(account_id)
        if conn is not None:
            return conn
        account = await self.api.metatrader_account_api.get_account(account_id)
        conn = account.get_rpc_connection()
        try:
            await conn.connect()
            await conn.wait_synchronized(60)
        except Exception:
            # Si connect()/wait_synchronized falla (p. ej. "Timed out waiting for
            # MetaApi to synchronize"), la conexión YA quedó medio abierta. Si no la
            # cerramos, cada ciclo fallido filtra una conexión: se acumulan, saturan
            # la sincronización de MetaApi y provocan MÁS timeouts (círculo vicioso).
            try:
                await conn.close()
            except Exception:  # noqa: BLE001
                pass
            raise
        self._conns[account_id] = conn
        return conn

    async def drop(self, account_id):
        conn = self._conns.pop(account_id, None)
        if conn is not None:
            try:
                await conn.close()
            except Exception:  # noqa: BLE001
                pass


def _direction_from_type(ptype):
    if ptype == "POSITION_TYPE_BUY":
        return "buy"
    if ptype == "POSITION_TYPE_SELL":
        return "sell"
    return None


def _streaming_ready(conn):
    """
    True si la conexión de streaming de la maestra está viva, SINCRONIZADA, su terminal
    conectado al bróker y —crucial— el websocket sigue RECIBIENDO datos. Solo cuando es
    True leemos sus posiciones de terminal_state y actuamos.

    El check del health monitor es la lección del 25-ago: el SDK puede seguir diciendo
    synchronized=True mientras el websocket dejó de recibir datos y terminal_state quedó
    CONGELADO (bug observado en cuentas con mucho historial) -> leeríamos posiciones
    viejas y no copiaríamos/cerraríamos. Si los quotes dejan de fluir, health_status
    marca healthy=False y aquí devolvemos False -> se cae al fallback por RPC (que sí
    trae un snapshot fresco). Nunca nos fiamos de un terminal_state posiblemente muerto.
    """
    if conn is None:
        return False
    if getattr(conn, "synchronized", None) is not True:
        return False
    ts = getattr(conn, "terminal_state", None)
    if ts is None:
        return False
    try:
        if not ts.connected_to_broker:
            return False
    except Exception:  # noqa: BLE001
        return False
    try:
        hm = getattr(conn, "health_monitor", None)
        status = hm.health_status if hm is not None else None
        # Si el monitor existe y dice que NO está sano (p. ej. dejaron de llegar
        # quotes = posible congelamiento), no confiamos en terminal_state.
        if status is not None and not status.get("healthy", False):
            return False
    except Exception:  # noqa: BLE001
        pass  # si no podemos leer el health, nos guiamos por synchronized + broker
    return True


def _slave_lot(master_lot, slave):
    """Lote a abrir en la esclava según su modo de copia (mín. 0.01)."""
    mode = slave.get("copy_mode", "multiplier")
    if mode == "fixed":
        lot = float(slave.get("fixed_lot") or 0.0)
    else:
        lot = float(master_lot) * float(slave.get("lot_multiplier") or 1.0)
    return max(round(lot, 2), 0.01)


async def _open_on_slave(conn, symbol, direction, lot, sl, tp, master_position_id):
    options = {"comment": f"copy:{master_position_id}"}
    if direction == "sell":
        result = await conn.create_market_sell_order(symbol, lot, sl, tp, options)
    else:
        result = await conn.create_market_buy_order(symbol, lot, sl, tp, options)
    return result.get("positionId") or result.get("orderId") or "unknown"


# --------------------------------------------------------------------------- #
#  Listener: cada evento de posición de la maestra dispara una reconciliación.
# --------------------------------------------------------------------------- #
class _MasterListener(SynchronizationListener):
    """Reacciona a los eventos de streaming de UNA cuenta maestra."""

    def __init__(self, manager, master_mid):
        super().__init__()
        self.manager = manager
        self.master_mid = master_mid

    def _trigger(self):
        # No bloqueamos el callback del SDK: encolamos una reconciliación coalescida
        # (varios eventos seguidos -> una sola reconciliación) para no disparar una
        # llamada por cada tick de precio.
        self.manager.schedule_reconcile(self.master_mid)

    async def on_positions_replaced(self, instance_index, positions):
        self._trigger()

    async def on_positions_updated(self, instance_index, positions, removed_positions_ids):
        self._trigger()

    async def on_position_updated(self, instance_index, position):
        self._trigger()

    async def on_position_removed(self, instance_index, position_id):
        self._trigger()


# --------------------------------------------------------------------------- #
#  Estado de copia de una maestra (vivo entre eventos).
# --------------------------------------------------------------------------- #
class MasterCtx:
    def __init__(self, master):
        self.master = master
        self.connection = None          # StreamingMetaApiConnectionInstance (websocket)
        self.lock = asyncio.Lock()
        self.reconcile_pending = False  # hay una reconciliación coalescida encolada
        # (slave_id, master_position_id) -> {"slave_position_id": str|None}
        self.copied = {}
        self.seed(master.get("open_trades", []))

    def update(self, master):
        """Refresca esclavas/open_trades sin perder lo copiado en esta sesión."""
        self.master = master
        self.seed(master.get("open_trades", []))

    def seed(self, open_trades):
        for t in open_trades:
            key = (t["slave_account_id"], str(t["master_position_id"]))
            self.copied.setdefault(key, {"slave_position_id": t.get("slave_position_id")})


# --------------------------------------------------------------------------- #
#  Manager: conexiones streaming por maestra + reconciliación + reporte.
# --------------------------------------------------------------------------- #
class CopyManager:
    def __init__(self, api, report_fn):
        self.api = api
        self.report_fn = report_fn
        self.pool = ConnectionPool(api)   # conexiones RPC a las esclavas (ejecutar órdenes)
        self.masters = {}                 # metaapi_account_id -> MasterCtx

    async def sync_config(self, masters):
        """Añade maestras nuevas, actualiza las existentes y quita las que ya no están."""
        incoming = {m["metaapi_account_id"]: m for m in masters}

        for mid in list(self.masters.keys()):
            if mid not in incoming:
                await self._remove_master(mid)

        for mid, m in incoming.items():
            if mid in self.masters:
                self.masters[mid].update(m)
                # Si el websocket se cayó/degradó, reengancharlo (best-effort).
                if not _streaming_ready(self.masters[mid].connection):
                    await self._attach_streaming(mid, self.masters[mid])
            else:
                await self._add_master(mid, m)
            await self.reconcile(mid)  # ponerse al día por si ya había posiciones

    async def _add_master(self, mid, master):
        ctx = MasterCtx(master)
        self.masters[mid] = ctx
        await self._attach_streaming(mid, ctx)

    async def _attach_streaming(self, mid, ctx):
        """
        Abre (o reabre) la conexión de STREAMING permanente de la maestra: es la que
        mantiene sus posiciones vivas en memoria (terminal_state) y empuja los eventos
        de apertura/cierre. Cierra primero cualquier streaming previo muerto.
        """
        old = ctx.connection
        ctx.connection = None
        if old is not None:
            try:
                await old.close()
            except Exception:  # noqa: BLE001
                pass
        try:
            account = await self.api.metatrader_account_api.get_account(mid)
            try:
                await account.reload()
            except Exception:  # noqa: BLE001
                pass
            if account.connection_status != "CONNECTED":
                log.warning("Copy: maestra %s aún no conectada al bróker (estado: %s); "
                            "se reintenta el streaming en el próximo refresco.",
                            mid, account.connection_status)
                return

            # history_start_time reciente = NO descargar el historial completo al
            # sincronizar. Para copiar solo necesitamos posiciones abiertas.
            recent = datetime.now(timezone.utc) - timedelta(hours=STREAM_HISTORY_HOURS)
            conn = account.get_streaming_connection(history_start_time=recent)
            conn.add_synchronization_listener(_MasterListener(self, mid))
            await conn.connect()
            # Asignamos la conexión YA: los eventos pueden empezar a llegar durante la
            # sincronización. Si el wait_synchronized tarda, seguimos: la copia usa el
            # fallback por RPC hasta que el websocket termine de sincronizar.
            ctx.connection = conn
            try:
                await asyncio.wait_for(conn.wait_synchronized(), timeout=STREAM_SYNC_TIMEOUT)
                log.info("Copy: websocket sincronizado con la maestra %s.", mid)
            except Exception:  # noqa: BLE001
                log.warning("Copy: el websocket de la maestra %s tardó en sincronizar; "
                            "se usa el fallback por RPC hasta que sincronice.", mid)
        except Exception as exc:  # noqa: BLE001
            log.warning("Copy: no se pudo abrir el websocket de la maestra %s (%s); "
                        "se reintenta en el próximo refresco.", mid, exc)

    async def _remove_master(self, mid):
        ctx = self.masters.pop(mid, None)
        if ctx and ctx.connection:
            try:
                await ctx.connection.close()
            except Exception:  # noqa: BLE001
                pass
        log.info("Copy: maestra %s desconectada (ya no tiene esclavas activas).", mid)

    async def reconcile_all(self):
        """Reconcilia TODAS las maestras (lo llama el poll de respaldo)."""
        for mid in list(self.masters.keys()):
            try:
                await self.reconcile(mid)
            except Exception:  # noqa: BLE001
                log.exception("Copy: error reconciliando la maestra %s", mid)

    def schedule_reconcile(self, mid):
        """
        Encola una reconciliación coalescida para la maestra: si ya hay una en cola, no
        encola otra (los eventos de streaming llegan en ráfaga —varios por segundo—).
        Un debounce corto agrupa la ráfaga en una sola reconciliación con el estado más
        reciente, manteniendo la latencia baja.
        """
        ctx = self.masters.get(mid)
        if ctx is None or ctx.reconcile_pending:
            return
        ctx.reconcile_pending = True
        asyncio.create_task(self._debounced_reconcile(mid))

    async def _debounced_reconcile(self, mid):
        try:
            await asyncio.sleep(0.3)  # agrupa la ráfaga de eventos (latencia baja)
        finally:
            ctx = self.masters.get(mid)
            if ctx is not None:
                ctx.reconcile_pending = False
        await self.reconcile(mid)

    async def _read_master_positions(self, mid, ctx):
        """
        Posiciones ABIERTAS reales de la maestra. Devuelve (positions, ok).
          - Fuente principal: terminal_state del websocket (en memoria, instantáneo).
          - Fallback: si el websocket no está listo, lectura PUNTUAL por RPC (fuente
            completa) para no detener la copia mientras sincroniza.
          - ok=False -> no se pudo leer con garantías: el llamador OMITE el ciclo (no
            abre ni cierra nada). Nunca actuamos sobre un estado desconocido.
        """
        conn = ctx.connection
        if _streaming_ready(conn):
            try:
                return list(conn.terminal_state.positions or []), True
            except Exception as exc:  # noqa: BLE001
                log.warning("Copy: no se pudo leer terminal_state de la maestra %s (%s); "
                            "se intenta por RPC.", mid, exc)

        # Fallback por RPC (solo lectura). Si falla, se omite el ciclo (no se cierra nada).
        try:
            mconn = await self.pool.get(mid)
            return (await mconn.get_positions() or []), True
        except Exception as exc:  # noqa: BLE001
            await self.pool.drop(mid)
            log.warning("Copy: maestra %s sin lectura fiable de posiciones (%s); "
                        "se omite este ciclo (no se cierra nada).", mid, exc)
            return [], False

    async def reconcile(self, mid):
        """Iguala las esclavas al estado real de la maestra (abre/cierra)."""
        ctx = self.masters.get(mid)
        if ctx is None:
            return

        async with ctx.lock:
            positions, ok = await self._read_master_positions(mid, ctx)
            if not ok:
                return  # no se pudo leer con garantías -> no tocamos nada

            master_pos = {str(p.get("id")): p for p in positions}
            master_ids = set(master_pos.keys())
            slaves = ctx.master.get("slaves", [])
            db_id = ctx.master["master_account_id"]

            opened, closed = [], []

            # 1) ABRIR lo que la maestra tiene y la esclava aún no copió.
            for slave in slaves:
                sid = slave["slave_account_id"]
                smid = slave["metaapi_account_id"]
                for mpid, pos in master_pos.items():
                    if (sid, mpid) in ctx.copied:
                        continue
                    direction = _direction_from_type(pos.get("type"))
                    if direction is None:
                        continue

                    symbol = pos.get("symbol")
                    master_lot = float(pos.get("volume") or 0.0)
                    lot = _slave_lot(master_lot, slave)
                    event = {
                        "master_account_id": db_id,
                        "slave_account_id": sid,
                        "master_position_id": mpid,
                        "symbol": symbol,
                        "direction": direction,
                        "master_lot": master_lot,
                        "slave_lot": lot,
                    }
                    try:
                        sconn = await self.pool.get(smid)
                        spid = await _open_on_slave(
                            sconn, symbol, direction, lot,
                            pos.get("stopLoss"), pos.get("takeProfit"), mpid,
                        )
                        event["slave_position_id"] = spid
                        event["status"] = "open"
                        ctx.copied[(sid, mpid)] = {"slave_position_id": spid}
                        log.info("Copy: abierta %s %s %s en esclava %s (maestra %s) -> %s",
                                 direction, symbol, lot, sid, mpid, spid)
                    except Exception as exc:  # noqa: BLE001
                        event["slave_position_id"] = None
                        event["status"] = "failed"
                        event["error"] = str(exc)
                        await self.pool.drop(smid)
                        log.warning("Copy: falló abrir en esclava %s (maestra %s): %s", sid, mpid, exc)
                    opened.append(event)

            # 2) CERRAR lo que la esclava copió pero la maestra ya cerró.
            for (sid, mpid), info in list(ctx.copied.items()):
                if mpid in master_ids:
                    continue  # la maestra la mantiene abierta
                slave = next((s for s in slaves if s["slave_account_id"] == sid), None)
                spid = info.get("slave_position_id")
                event = {"slave_account_id": sid, "master_position_id": mpid}

                if slave is None or not spid:
                    event["status"] = "closed"  # nada que cerrar en el broker
                    ctx.copied.pop((sid, mpid), None)
                    closed.append(event)
                    continue

                try:
                    sconn = await self.pool.get(slave["metaapi_account_id"])
                    await sconn.close_position(spid)
                    event["status"] = "closed"
                    ctx.copied.pop((sid, mpid), None)
                    log.info("Copy: cerrada %s en esclava %s (maestra cerró %s).", spid, sid, mpid)
                except Exception as exc:  # noqa: BLE001
                    event["status"] = "failed"
                    event["error"] = str(exc)
                    await self.pool.drop(slave["metaapi_account_id"])
                    log.warning("Copy: falló cerrar %s en esclava %s: %s", spid, sid, exc)
                closed.append(event)

            if opened or closed:
                try:
                    self.report_fn(opened, closed)
                except Exception as exc:  # noqa: BLE001
                    log.warning("Copy: no se pudo reportar al panel (%s).", exc)
