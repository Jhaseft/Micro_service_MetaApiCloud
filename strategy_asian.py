"""
strategy_asian.py
-----------------
Estrategia "Asian Range Breakout" (edición prop firm), portada del EA de MT5
`PropFirmBreakoutEA.mq5` al worker de MetaApi Cloud.

Idea: durante la sesión asiática se forma un rango (máximo/mínimo). Cuando, ya en
sesión de Londres o Nueva York, la última vela CIERRA por encima del rango (+buffer)
se compra; si cierra por debajo (-buffer) se vende. Se exige además un repunte de
volumen para confirmar la ruptura.

Decide la dirección (buy / sell / None) y, cuando hay señal, devuelve los niveles
para que el worker calcule SL/TP basados en el rango (no en pips fijos):
    - SL: al otro lado del rango +/- buffer, con un piso mínimo de ATR.
    - TP: a una distancia = SL * tp_rr_multiplier (ratio R:R).

Todos los umbrales son parámetros que el cliente edita desde el panel (estrategia
"asian_breakout"). Python puro, sin dependencias, para mantener el worker liviano.

NO portado todavía (gestión prop firm que necesita estado entre ciclos / equity):
  - control de drawdown diario (max_daily_loss_pct) y halt de emergencia,
  - tope de operaciones por día (max_daily_trades),
  - lotaje por % de riesgo (risk_per_trade_pct) -> el worker usa el lotaje fijo,
  - ciclo de relajación de filtros / entrada forzada (relax_*, force_*).
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Acceso a campos de la vela (MetaApi: open/high/low/close + tickVolume + brokerTime)
# ---------------------------------------------------------------------------
def _high(c: Dict[str, Any]) -> float:
    return float(c["high"])


def _low(c: Dict[str, Any]) -> float:
    return float(c["low"])


def _close(c: Dict[str, Any]) -> float:
    return float(c["close"])


def _vol(c: Dict[str, Any]) -> float:
    return float(c.get("tickVolume", c.get("volume", 0)) or 0)


def broker_hour(candle: Dict[str, Any]) -> Optional[int]:
    """Hora (0-23) en tiempo del broker. brokerTime viene como 'YYYY-MM-DD HH:MM:SS...'."""
    bt = candle.get("brokerTime")
    if isinstance(bt, str) and len(bt) >= 13:
        try:
            return int(bt[11:13])
        except ValueError:
            return None
    return None


def _broker_date(candle: Dict[str, Any]) -> Optional[str]:
    bt = candle.get("brokerTime")
    if isinstance(bt, str) and len(bt) >= 10:
        return bt[:10]
    return None


# ---------------------------------------------------------------------------
# Indicadores
# ---------------------------------------------------------------------------
def atr(candles: List[Dict[str, Any]], period: int) -> Optional[float]:
    """ATR simple (media de los true range de las últimas `period` velas)."""
    if period < 1 or len(candles) < period + 1:
        return None
    trs: List[float] = []
    for i in range(len(candles) - period, len(candles)):
        high, low = _high(candles[i]), _low(candles[i])
        prev_close = _close(candles[i - 1])
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    return sum(trs) / len(trs) if trs else None


# ---------------------------------------------------------------------------
# Decisión
# ---------------------------------------------------------------------------
def decide(
    candles: List[Dict[str, Any]],
    params: Dict[str, Any],
    *,
    server_hour: Optional[int],
    pip_size: float,
) -> Tuple[Optional[str], str, Optional[Dict[str, float]]]:
    """
    candles: velas del timeframe del bot, de más antigua a más reciente.
    params:  parámetros de la estrategia 'asian_breakout' (fusionados con defaults).
    server_hour: hora actual en tiempo del broker (de la última vela).
    pip_size: tamaño de un pip en precio (point * factor según dígitos).

    Devuelve (direction, reason, levels). Si no hay señal, direction=None y
    levels=None. Si hay señal, levels = {range_high, range_low, eff_buffer,
    atr_floor, rr} (todo en precio) para que el worker calcule SL/TP.
    """
    if len(candles) < 10:
        return None, "datos insuficientes", None
    if server_hour is None:
        return None, "sin hora del broker en las velas", None

    asian_start = int(params.get("asian_start_hour", 0))
    asian_end = int(params.get("asian_end_hour", 7))
    london_start = int(params.get("london_start_hour", 8))
    london_end = int(params.get("london_end_hour", 10))
    ny_start = int(params.get("ny_start_hour", 15))
    ny_end = int(params.get("ny_end_hour", 17))
    sessions_only = bool(params.get("trade_sessions_only", True))

    # --- 1) Filtro de sesión: solo operar en Londres o NY (tras la sesión asiática).
    in_london = london_start <= server_hour < london_end
    in_ny = ny_start <= server_hour < ny_end
    if sessions_only and not (in_london or in_ny):
        return None, f"fuera de sesión Londres/NY (hora broker {server_hour})", None
    session = "LONDON" if in_london else ("NY" if in_ny else "OFF")

    # --- 2) Rango asiático del día actual (fecha de la última vela).
    today = _broker_date(candles[-1])
    asian_high = 0.0
    asian_low = float("inf")
    for c in candles:
        if _broker_date(c) != today:
            continue
        hour = broker_hour(c)
        if hour is None or not (asian_start <= hour < asian_end):
            continue
        asian_high = max(asian_high, _high(c))
        asian_low = min(asian_low, _low(c))

    if asian_high <= 0 or asian_low == float("inf") or asian_high <= asian_low:
        return None, "rango asiático no disponible (finde o pocos datos)", None

    # --- 3) Ruptura de la última vela cerrada (+/- buffer).
    last = candles[-1]
    close = _close(last)
    eff_buffer = float(params.get("breakout_buffer_pips", 2.0)) * pip_size
    break_up = close > asian_high + eff_buffer
    break_down = close < asian_low - eff_buffer
    if not break_up and not break_down:
        rng = (asian_high - asian_low) / pip_size if pip_size else 0
        return None, f"sin ruptura del rango ({rng:.1f} pips, {session})", None

    # --- 4) Filtro de volumen: la vela de ruptura debe superar el volumen medio.
    lookback = int(params.get("volume_lookback", 20))
    prev = candles[-(lookback + 1):-1] if lookback > 0 else []
    avg_vol = sum(_vol(c) for c in prev) / len(prev) if prev else 0.0
    surge_mult = float(params.get("volume_surge_multiplier", 2.0))
    cur_vol = _vol(last)
    if avg_vol <= 0:
        return None, "volumen medio inválido", None
    if cur_vol < avg_vol * surge_mult:
        return None, (
            f"volumen {cur_vol:.0f} < umbral {avg_vol * surge_mult:.0f} "
            f"(x{surge_mult})"
        ), None

    # --- 5) Señal válida. Calcula piso de SL por ATR y arma los niveles.
    atr_value = atr(candles, int(params.get("atr_period", 14))) or 0.0
    atr_floor = atr_value * float(params.get("atr_sl_floor_mult", 1.0))
    rr = float(params.get("tp_rr_multiplier", 2.0))

    direction = "buy" if break_up else "sell"
    levels = {
        "range_high": asian_high,
        "range_low": asian_low,
        "eff_buffer": eff_buffer,
        "atr_floor": atr_floor,
        "rr": rr,
    }
    reason = f"ruptura {('alcista' if break_up else 'bajista')} del rango [{session}]"
    return direction, reason, levels
