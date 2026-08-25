"""
copy_trade.py — Copia automática maestra -> esclavas (open + close) por EVENTOS.

A diferencia de la versión anterior (que hacía polling con get_positions cada N
segundos), ahora abrimos una conexión de STREAMING a cada cuenta maestra y
MetaApi nos EMPUJA los eventos en el momento exacto en que la maestra abre o
cierra una posición. Latencia de milisegundos y sin polling de operaciones.

Flujo:
  - El panel (fuente de verdad de la CONFIG) expone en /api/worker/copy-accounts
    cada maestra con sus esclavas y las operaciones ya copiadas (open_trades).
  - CopyManager mantiene, best-effort, una conexión streaming por maestra + un
    listener SOLO para disparar la reconciliación con baja latencia.
  - reconcile() lee el estado REAL de la maestra por RPC (get_positions) —esa es
    la fuente de verdad de las operaciones— y abre lo que falta / cierra lo que la
    maestra cerró, en cada esclava (vía RPC). Reporta al panel (POST /copy-trades).

Importante (robustez con dinero real): la copia NO depende de que el streaming
esté sano. Si la conexión de streaming se degrada o muere, el poll de
configuración cada 60s (metaapi_worker.copy_loop) igual ejecuta reconcile(), que
al leer por RPC refleja el estado real. El streaming solo acelera la reacción;
si falla, se reengancha solo en el siguiente ciclo. Y si el RPC falla, se OMITE
el ciclo sin cerrar nada (nunca cerramos por un error de lectura).
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from metaapi_cloud_sdk import SynchronizationListener

log = logging.getLogger("eas-worker")


# --------------------------------------------------------------------------- #
#  Conexiones RPC a las esclavas (para ejecutar órdenes), cacheadas.
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
        await conn.connect()
        await conn.wait_synchronized(60)
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


def _connection_ready(conn):
    """
    True si la conexión de streaming está viva y sincronizada. Solo se usa para
    decidir si hace falta RECONECTAR el streaming (que sirve para disparar la
    copia con baja latencia). La CORRECCIÓN de la copia NO depende de esto: el
    snapshot de la maestra se lee por RPC en cada reconciliación.
    """
    if conn is None:
        return False
    if getattr(conn, "synchronized", None) is False:
        return False
    ts = getattr(conn, "terminal_state", None)
    if ts is not None:
        if getattr(ts, "connected_to_broker", True) is False:
            return False
        if getattr(ts, "connected", True) is False:
            return False
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
        # llamada RPC por cada tick de precio.
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
        self.connection = None
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
        self.pool = ConnectionPool(api)   # conexiones RPC a las esclavas
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
                # Si el streaming se cayó/degradó, reengancharlo (best-effort, solo
                # para latencia). La copia funciona igual por el poll+RPC de abajo.
                if not _connection_ready(self.masters[mid].connection):
                    await self._attach_streaming(mid, self.masters[mid])
            else:
                await self._add_master(mid, m)
            await self.reconcile(mid)  # ponerse al día por si ya había posiciones

    async def _add_master(self, mid, master):
        # La maestra se registra SIEMPRE: la copia se hace por RPC en reconcile(),
        # así que no depende de que el streaming conecte. El streaming es un extra
        # de baja latencia que se engancha aparte (best-effort).
        ctx = MasterCtx(master)
        self.masters[mid] = ctx
        await self._attach_streaming(mid, ctx)

    async def _attach_streaming(self, mid, ctx):
        """
        Abre (o reabre) la conexión de STREAMING de la maestra, solo para disparar
        la reconciliación con baja latencia cuando abre/cierra. Es best-effort: si
        falla, la copia sigue funcionando por el poll de config (cada 60s) que lee
        el estado real por RPC. Cierra primero cualquier streaming previo muerto.
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
                            "se copiará por poll y se reintenta el streaming luego.",
                            mid, account.connection_status)
                return

            # history_start_time reciente = NO descargar el historial completo de la
            # cuenta al sincronizar el streaming. En cuentas con mucho historial
            # (miles de operaciones) esa descarga hacía que la sincronización
            # "no terminara a tiempo" y el streaming quedara en bucle de resync.
            # Para copiar solo necesitamos posiciones, no historial.
            recent = datetime.now(timezone.utc) - timedelta(hours=1)
            conn = account.get_streaming_connection(history_start_time=recent)
            conn.add_synchronization_listener(_MasterListener(self, mid))
            await conn.connect()
            # Asignamos la conexión YA: los eventos pueden empezar a llegar durante
            # la sincronización. No bloqueamos indefinidamente en wait_synchronized
            # (hay cuentas cuyo streaming "no termina de sincronizar a tiempo"): si
            # tarda, seguimos —la copia va por el poll+RPC, esto es solo latencia—.
            ctx.connection = conn
            try:
                await asyncio.wait_for(conn.wait_synchronized(), timeout=30)
                log.info("Copy: streaming conectado a la maestra %s.", mid)
            except Exception:  # noqa: BLE001
                log.warning("Copy: streaming de la maestra %s tardó en sincronizar; "
                            "se usa el poll por RPC mientras tanto.", mid)
        except Exception as exc:  # noqa: BLE001
            log.warning("Copy: streaming no disponible para maestra %s (%s); "
                        "se copiará por poll cada 60s.", mid, exc)

    async def _remove_master(self, mid):
        ctx = self.masters.pop(mid, None)
        if ctx and ctx.connection:
            try:
                await ctx.connection.close()
            except Exception:  # noqa: BLE001
                pass
        log.info("Copy: maestra %s desconectada (ya no tiene esclavas activas).", mid)

    async def reconcile_all(self):
        """Reconcilia TODAS las maestras (lo llama el poll rápido por RPC)."""
        for mid in list(self.masters.keys()):
            try:
                await self.reconcile(mid)
            except Exception:  # noqa: BLE001
                log.exception("Copy: error reconciliando la maestra %s", mid)

    def schedule_reconcile(self, mid):
        """
        Encola una reconciliación coalescida para la maestra: si ya hay una en cola,
        no encola otra (los eventos de streaming pueden llegar en ráfaga —varios por
        segundo— y cada reconcile hace una lectura RPC). Un debounce corto agrupa la
        ráfaga en una sola reconciliación que ya leerá el estado más reciente.
        """
        ctx = self.masters.get(mid)
        if ctx is None or ctx.reconcile_pending:
            return
        ctx.reconcile_pending = True
        asyncio.create_task(self._debounced_reconcile(mid))

    async def _debounced_reconcile(self, mid):
        try:
            await asyncio.sleep(1.0)  # agrupa la ráfaga de eventos
        finally:
            ctx = self.masters.get(mid)
            if ctx is not None:
                ctx.reconcile_pending = False
        await self.reconcile(mid)

    async def reconcile(self, mid):
        """Iguala las esclavas al estado real de la maestra (abre/cierra)."""
        ctx = self.masters.get(mid)
        if ctx is None:
            return

        async with ctx.lock:
            # FUENTE DE VERDAD = posiciones REALES de la maestra por RPC. No usamos
            # el terminal_state del streaming porque puede quedar "congelado" si la
            # conexión se degrada en silencio (bug observado: la maestra dejaba de
            # copiar). El RPC refleja el estado real del bróker en este instante.
            # Si el RPC falla, se OMITE el ciclo (no se abre ni se cierra) — nunca
            # cerramos por un error de lectura (dinero real).
            try:
                mconn = await self.pool.get(mid)
                positions = await mconn.get_positions() or []
            except Exception as exc:  # noqa: BLE001
                await self.pool.drop(mid)
                log.warning("Copy: no se pudieron leer las posiciones de la maestra %s "
                            "(%s); se omite este ciclo (no se cierra nada).", mid, exc)
                return

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
