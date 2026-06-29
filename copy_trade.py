"""
copy_trade.py — Copia automática maestra -> esclavas (open + close) por EVENTOS.

A diferencia de la versión anterior (que hacía polling con get_positions cada N
segundos), ahora abrimos una conexión de STREAMING a cada cuenta maestra y
MetaApi nos EMPUJA los eventos en el momento exacto en que la maestra abre o
cierra una posición. Latencia de milisegundos y sin polling de operaciones.

Flujo:
  - El panel (fuente de verdad) expone en /api/worker/copy-accounts cada maestra
    con sus esclavas y las operaciones ya copiadas (open_trades).
  - CopyManager mantiene una conexión streaming viva por maestra + un listener.
  - Cuando llega un evento de posición -> reconcile(): compara el snapshot real
    de la maestra (terminal_state.positions) con lo ya copiado y abre lo que
    falta / cierra lo que la maestra cerró, en cada esclava (vía RPC).
  - Reporta cada acción al panel (POST /api/worker/copy-trades).

El único "polling" que queda es refrescar la configuración (quién sigue a quién)
cada 60s; eso lo hace metaapi_worker.copy_loop, no este módulo.
"""

import asyncio
import logging

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
        # No bloqueamos el callback del SDK: lanzamos la reconciliación aparte.
        asyncio.create_task(self.manager.reconcile(self.master_mid))

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
            else:
                await self._add_master(mid, m)
            await self.reconcile(mid)  # ponerse al día por si ya había posiciones

    async def _add_master(self, mid, master):
        ctx = MasterCtx(master)
        try:
            account = await self.api.metatrader_account_api.get_account(mid)

            # No intentar suscribir si el terminal aún no conectó al bróker:
            # evita timeouts y tracebacks de "not connected to broker yet".
            try:
                await account.reload()
            except Exception:  # noqa: BLE001
                pass
            if account.connection_status != "CONNECTED":
                log.warning("Copy: maestra %s aún no conectada al bróker (estado: %s); se reintenta luego.",
                            mid, account.connection_status)
                return

            conn = account.get_streaming_connection()
            conn.add_synchronization_listener(_MasterListener(self, mid))
            await conn.connect()
            await conn.wait_synchronized()
            ctx.connection = conn
            self.masters[mid] = ctx
            log.info("Copy: streaming conectado a la maestra %s.", mid)
        except Exception as exc:  # noqa: BLE001
            log.warning("Copy: no se pudo conectar el streaming de la maestra %s (%s).", mid, exc)

    async def _remove_master(self, mid):
        ctx = self.masters.pop(mid, None)
        if ctx and ctx.connection:
            try:
                await ctx.connection.close()
            except Exception:  # noqa: BLE001
                pass
        log.info("Copy: maestra %s desconectada (ya no tiene esclavas activas).", mid)

    async def reconcile(self, mid):
        """Iguala las esclavas al estado real de la maestra (abre/cierra)."""
        ctx = self.masters.get(mid)
        if ctx is None or ctx.connection is None:
            return

        async with ctx.lock:
            try:
                positions = ctx.connection.terminal_state.positions or []
            except Exception as exc:  # noqa: BLE001
                log.warning("Copy: sin snapshot de la maestra %s (%s).", mid, exc)
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
