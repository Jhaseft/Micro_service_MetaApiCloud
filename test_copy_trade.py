"""
Pruebas OFFLINE de la lógica de copia (copy_trade.CopyManager.reconcile).

No tocan MetaApi ni la red: simulan la conexión de streaming de la maestra
(terminal_state + health_monitor), el fallback por RPC y las conexiones RPC de
las esclavas. Validan lo crítico con dinero real:
  - abrir en la esclava lo que la maestra abre (con multiplicador de lote),
  - cerrar en la esclava cuando la maestra cierra,
  - usar el fallback por RPC cuando el websocket no está sano (freeze del 25-ago),
  - NUNCA cerrar una copia si no se pudo leer el estado de la maestra.
Ejecutar:  python test_copy_trade.py
"""

import asyncio
import time

import copy_trade


# --------------------------- Fakes (sin red) ------------------------------- #
class FakeTerminalState:
    def __init__(self, positions, connected_to_broker=True, connected=True):
        self._positions = positions
        self.connected_to_broker = connected_to_broker
        self.connected = connected

    @property
    def positions(self):
        return self._positions


class FakeHealthMonitor:
    def __init__(self, healthy=True):
        self._healthy = healthy

    @property
    def health_status(self):
        return {"healthy": self._healthy, "quoteStreamingHealthy": self._healthy,
                "synchronized": True, "connected": True, "connectedToBroker": True, "message": ""}


class FakeStreamingConn:
    """Simula StreamingMetaApiConnectionInstance (maestra)."""
    def __init__(self, positions, synchronized=True, healthy=True, connected_to_broker=True, connected=True):
        self.synchronized = synchronized
        self.terminal_state = FakeTerminalState(positions, connected_to_broker, connected)
        self.health_monitor = FakeHealthMonitor(healthy)

    def set_positions(self, positions):
        self.terminal_state._positions = positions


class FakeMasterRpc:
    """Fallback por RPC de la maestra (get_positions)."""
    def __init__(self, positions=None, fail=False):
        self._positions = positions or []
        self.fail = fail

    async def get_positions(self):
        if self.fail:
            raise RuntimeError("RPC timeout simulado")
        return self._positions


class FakeSlaveConn:
    """Simula la conexión RPC de una esclava (ejecutar órdenes)."""
    def __init__(self):
        self.opened = []
        self.closed = []
        self._next_id = 1000

    async def create_market_buy_order(self, symbol, lot, sl, tp, options):
        self._next_id += 1
        self.opened.append({"dir": "buy", "symbol": symbol, "lot": lot, "opt": options})
        return {"positionId": str(self._next_id)}

    async def create_market_sell_order(self, symbol, lot, sl, tp, options):
        self._next_id += 1
        self.opened.append({"dir": "sell", "symbol": symbol, "lot": lot, "opt": options})
        return {"positionId": str(self._next_id)}

    async def close_position(self, spid):
        self.closed.append(spid)


class FakePool:
    def __init__(self, conns):
        self._conns = conns
        self.dropped = []

    async def get(self, account_id):
        conn = self._conns.get(account_id)
        if conn is None:
            raise RuntimeError(f"sin conexión para {account_id}")
        return conn

    async def drop(self, account_id):
        self.dropped.append(account_id)


def _pos(pid, ptype, symbol, volume):
    return {"id": pid, "type": ptype, "symbol": symbol, "volume": volume,
            "stopLoss": None, "takeProfit": None}


def _make_manager(master_conn, slave_conn, master_rpc=None):
    reports = []
    mgr = copy_trade.CopyManager(api=None, report_fn=lambda o, c: reports.append((o, c)))
    conns = {"slave-mid": slave_conn}
    if master_rpc is not None:
        conns["master-mid"] = master_rpc
    mgr.pool = FakePool(conns)

    ctx = copy_trade.MasterCtx({
        "master_account_id": 4,
        "slaves": [{
            "slave_account_id": 44,
            "metaapi_account_id": "slave-mid",
            "copy_mode": "multiplier",
            "lot_multiplier": 2.0,
            "fixed_lot": None,
        }],
        "open_trades": [],
    })
    ctx.connection = master_conn
    mgr.masters["master-mid"] = ctx
    return mgr, ctx, reports


# ------------------------------ Tests -------------------------------------- #
async def test_open_via_streaming():
    master = FakeStreamingConn([_pos("111", "POSITION_TYPE_BUY", "XAUUSD", 0.01)])
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave)

    await mgr.reconcile("master-mid")

    assert len(slave.opened) == 1, slave.opened
    assert slave.opened[0]["dir"] == "buy"
    assert slave.opened[0]["symbol"] == "XAUUSD"
    assert slave.opened[0]["lot"] == 0.02, "multiplicador 2 sobre 0.01"
    assert (44, "111") in ctx.copied
    assert reports and reports[0][0][0]["status"] == "open"
    print("OK  test_open_via_streaming (abre 0.02 buy en la esclava)")


async def test_close_when_master_closes():
    master = FakeStreamingConn([_pos("111", "POSITION_TYPE_BUY", "XAUUSD", 0.01)])
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave)

    await mgr.reconcile("master-mid")          # abre
    master.set_positions([])                    # la maestra cierra todo
    await mgr.reconcile("master-mid")           # debe cerrar en la esclava

    assert len(slave.closed) == 1, slave.closed
    assert (44, "111") not in ctx.copied
    print("OK  test_close_when_master_closes (cierra la copia en la esclava)")


async def test_no_duplicate_open():
    master = FakeStreamingConn([_pos("111", "POSITION_TYPE_BUY", "XAUUSD", 0.01)])
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave)

    await mgr.reconcile("master-mid")
    await mgr.reconcile("master-mid")           # segunda vuelta: no debe reabrir
    await mgr.reconcile("master-mid")

    assert len(slave.opened) == 1, "no debe duplicar la copia de la misma posición"
    print("OK  test_no_duplicate_open (idempotente: no reabre)")


async def test_freeze_falls_back_to_rpc():
    # Streaming CONGELADO (healthy=False) pero la maestra SÍ tiene la posición:
    # debe leerla por el fallback RPC y copiar igual.
    master = FakeStreamingConn([], healthy=False)     # terminal_state "muerto"
    master_rpc = FakeMasterRpc([_pos("222", "POSITION_TYPE_SELL", "USTEC_x100", 0.01)])
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave, master_rpc=master_rpc)

    await mgr.reconcile("master-mid")

    assert len(slave.opened) == 1, "el fallback RPC debe permitir copiar"
    assert slave.opened[0]["dir"] == "sell"
    print("OK  test_freeze_falls_back_to_rpc (congelado -> copia por RPC)")


async def test_no_close_on_unreadable_state():
    # CRÍTICO: si NO se puede leer el estado de la maestra (streaming no sano Y RPC
    # falla), NUNCA se debe cerrar la copia existente.
    master = FakeStreamingConn([_pos("333", "POSITION_TYPE_BUY", "EURUSD", 0.01)])
    master_rpc = FakeMasterRpc(fail=True)
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave, master_rpc=master_rpc)

    await mgr.reconcile("master-mid")                 # abre con streaming sano
    assert (44, "333") in ctx.copied

    master.health_monitor._healthy = False            # streaming se degrada
    master.set_positions([])                          # (da igual: no debemos fiarnos)
    await mgr.reconcile("master-mid")                 # RPC falla -> NO cerrar

    assert len(slave.closed) == 0, "NUNCA cerrar si no se pudo leer la maestra"
    assert (44, "333") in ctx.copied, "la copia sigue registrada"
    assert "master-mid" in mgr.pool.dropped, "se soltó la conexión RPC fallida"
    print("OK  test_no_close_on_unreadable_state (no cierra ante lectura fallida)")


async def test_streaming_ready_guards():
    assert copy_trade._streaming_ready(None) is False
    assert copy_trade._streaming_ready(FakeStreamingConn([], synchronized=False)) is False
    assert copy_trade._streaming_ready(FakeStreamingConn([], connected_to_broker=False)) is False
    assert copy_trade._streaming_ready(FakeStreamingConn([], healthy=False)) is False
    assert copy_trade._streaming_ready(FakeStreamingConn([], healthy=True)) is True
    print("OK  test_streaming_ready_guards (sync + broker + health)")


async def test_streaming_alive_vs_ready():
    # Vivo pero SIN sincronizar: NO se debe reenganchar (dejarlo terminar) aunque
    # no esté "ready". Solo se reengancha si se cayó (connected=False).
    syncing = FakeStreamingConn([], synchronized=False, connected=True)
    assert copy_trade._streaming_alive(syncing) is True, "vivo aunque sincronizando"
    assert copy_trade._streaming_ready(syncing) is False, "no listo para leer aún"

    dead = FakeStreamingConn([], connected=False)
    assert copy_trade._streaming_alive(dead) is False, "caído -> reenganchar"
    assert copy_trade._streaming_alive(None) is False
    print("OK  test_streaming_alive_vs_ready (no reconstruye si sigue sincronizando)")


async def test_giveup_streaming_to_rpc_only():
    # Websocket vivo pero que NUNCA sincroniza (cuenta pesada). Al pasar el deadline,
    # reconcile lo descarta a RPC-only y copia por el fallback RPC.
    import copy_trade as ct
    closed_flag = {"v": False}

    class NeverSyncStream:
        def __init__(self):
            self.synchronized = False
            self.terminal_state = FakeTerminalState([], connected=True)
            self.health_monitor = FakeHealthMonitor(healthy=False)
        async def close(self):
            closed_flag["v"] = True

    master = NeverSyncStream()
    master_rpc = FakeMasterRpc([_pos("777", "POSITION_TYPE_BUY", "XAUUSD", 0.01)])
    slave = FakeSlaveConn()
    mgr, ctx, reports = _make_manager(master, slave, master_rpc=master_rpc)
    ctx.stream_deadline = time.monotonic() - 1  # ya venció

    await mgr.reconcile("master-mid")

    assert ctx.rpc_only is True, "debe pasar a RPC-only"
    assert ctx.connection is None
    assert closed_flag["v"] is True, "debe cerrar el websocket descartado"
    assert len(slave.opened) == 1, "copia por RPC tras descartar el websocket"
    print("OK  test_giveup_streaming_to_rpc_only (auto-degradación a RPC)")


async def main():
    await test_streaming_ready_guards()
    await test_streaming_alive_vs_ready()
    await test_giveup_streaming_to_rpc_only()
    await test_open_via_streaming()
    await test_close_when_master_closes()
    await test_no_duplicate_open()
    await test_freeze_falls_back_to_rpc()
    await test_no_close_on_unreadable_state()
    print("\nTODAS LAS PRUEBAS PASARON")


if __name__ == "__main__":
    asyncio.run(main())
