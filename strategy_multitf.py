"""
strategy_multitf.py
-------------------
Estrategia "Multi-TF Order Flow". Decide la dirección de entrada (buy / sell /
None) a partir de:

  1. Tendencia alineada por EMA en D1, H8 y H4 (H8 sintético desde H4).
  2. Order Flow en M5: vela con volumen por encima de su media y cuerpo amplio.

Solo abre cuando la tendencia de los 3 timeframes está alineada Y el order flow
de M5 apunta en la misma dirección. Todos los umbrales son parámetros que el
cliente edita desde el panel (estrategia "multitf_orderflow").

Python puro (sin pandas/numpy) para mantener el worker liviano.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Acceso a campos de la vela (MetaApi devuelve open/high/low/close + tickVolume)
# ---------------------------------------------------------------------------
def _vol(candle: Dict[str, Any]) -> float:
    return float(candle.get("tickVolume", candle.get("volume", 0)) or 0)


def _closes(candles: List[Dict[str, Any]]) -> List[float]:
    return [float(c["close"]) for c in candles]


# ---------------------------------------------------------------------------
# Indicadores
# ---------------------------------------------------------------------------
def ema_last(values: List[float], period: int) -> Optional[float]:
    """EMA recursiva; devuelve el último valor. None si no hay datos."""
    if not values:
        return None
    k = 2.0 / (period + 1)
    e = values[0]
    for v in values[1:]:
        e = v * k + e * (1 - k)
    return e


def sma_last(values: List[float], period: int) -> float:
    """SMA de las últimas `period` muestras (o de las que haya)."""
    if not values:
        return 0.0
    window = values[-period:] if len(values) >= period else values
    return sum(window) / len(window)


def body_ratio(candle: Dict[str, Any]) -> float:
    """Proporción cuerpo/rango de la vela. 0 si la vela no tiene rango."""
    high = float(candle["high"])
    low = float(candle["low"])
    rng = high - low
    if rng <= 0:
        return 0.0
    body = abs(float(candle["close"]) - float(candle["open"]))
    return body / rng


# ---------------------------------------------------------------------------
# H8 sintético desde H4
# ---------------------------------------------------------------------------
def build_h8_from_h4(h4: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Agrupa velas H4 de a pares (2 H4 = 1 H8). h4: más antigua -> más reciente."""
    out: List[Dict[str, Any]] = []
    start = len(h4) % 2  # arranca en un par alineado
    pair: List[Dict[str, Any]] = []
    for c in h4[start:]:
        pair.append(c)
        if len(pair) == 2:
            out.append({
                "time": pair[0]["time"],
                "open": pair[0]["open"],
                "high": max(float(pair[0]["high"]), float(pair[1]["high"])),
                "low": min(float(pair[0]["low"]), float(pair[1]["low"])),
                "close": pair[1]["close"],
                "tickVolume": _vol(pair[0]) + _vol(pair[1]),
            })
            pair = []
    return out


# ---------------------------------------------------------------------------
# Tendencia multi-timeframe
# ---------------------------------------------------------------------------
def trend_bias(
    d1: List[Dict[str, Any]],
    h8: List[Dict[str, Any]],
    h4: List[Dict[str, Any]],
    ema_period: int,
) -> str:
    """'buy' si los 3 cierres > EMA; 'sell' si los 3 < EMA; si no, 'neutral'."""
    aboves = []
    for candles in (d1, h8, h4):
        closes = _closes(candles)
        e = ema_last(closes, ema_period)
        if not closes or e is None:
            return "neutral"
        aboves.append(closes[-1] > e)

    if all(aboves):
        return "buy"
    if not any(aboves):
        return "sell"
    return "neutral"


# ---------------------------------------------------------------------------
# Order Flow en M5
# ---------------------------------------------------------------------------
def order_flow_signal(
    m5: List[Dict[str, Any]],
    vol_multiplier: float,
    min_body_ratio: float,
    sma_period: int,
) -> str:
    """'bullish' / 'bearish' / 'none' sobre la última vela cerrada de M5."""
    if not m5:
        return "none"

    volumes = [_vol(c) for c in m5]
    last = m5[-1]
    last_vol = volumes[-1]
    vol_sma = sma_last(volumes, sma_period)
    vol_ratio = (last_vol / vol_sma) if vol_sma > 0 else 0.0

    is_bull = float(last["close"]) > float(last["open"])
    is_bear = float(last["close"]) < float(last["open"])

    vol_ok = vol_ratio >= vol_multiplier
    body_ok = body_ratio(last) >= min_body_ratio

    if is_bull and vol_ok and body_ok:
        return "bullish"
    if is_bear and vol_ok and body_ok:
        return "bearish"
    return "none"


# ---------------------------------------------------------------------------
# Decisión final
# ---------------------------------------------------------------------------
def _num(params: Dict[str, Any], key: str, default: float) -> float:
    try:
        return float(params.get(key, default))
    except (TypeError, ValueError):
        return default


def _int(params: Dict[str, Any], key: str, default: int) -> int:
    try:
        return int(float(params.get(key, default)))
    except (TypeError, ValueError):
        return default


def decide(
    d1: List[Dict[str, Any]],
    h4: List[Dict[str, Any]],
    m5: List[Dict[str, Any]],
    params: Dict[str, Any],
) -> Tuple[Optional[str], str]:
    """
    Devuelve (direction, reason) donde direction es 'buy', 'sell' o None.
    `params` es el JSON de parámetros del bot tal cual viene del panel.
    """
    ema_period = _int(params, "ema_trend_period", 50)
    vol_mult = _num(params, "of_volume_multiplier", 1.5)
    min_body = _num(params, "of_min_body_ratio", 0.60)
    sma_period = _int(params, "of_volume_sma_period", 20)

    h8 = build_h8_from_h4(h4)

    bias = trend_bias(d1, h8, h4, ema_period)
    if bias == "neutral":
        return None, "tendencia D1/H8/H4 no alineada"

    of = order_flow_signal(m5, vol_mult, min_body, sma_period)

    if bias == "buy" and of == "bullish":
        return "buy", "tendencia alcista + order flow alcista"
    if bias == "sell" and of == "bearish":
        return "sell", "tendencia bajista + order flow bajista"

    return None, f"order flow ({of}) no confirma la tendencia ({bias})"
