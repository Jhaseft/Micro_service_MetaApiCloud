"""
test_strategies.py — Prueba rápida de las estrategias en Python (sin tocar MetaApi).

Verifica que la LÓGICA de decisión funciona, alimentando a cada estrategia con
velas "de laboratorio" en las que sabemos qué debería pasar, y comprobando que
devuelve lo esperado (comprar / vender / no operar).

NO conecta a ningún broker ni abre operaciones: solo prueba el cerebro.

Cómo correrlo:
    python test_strategies.py

Si todo está bien, termina con "TODAS LAS PRUEBAS PASARON" y código 0.
"""

import sys

import strategy_asian
import strategy_multitf

# Contador global de resultados.
_passed = 0
_failed = 0


def check(nombre, condicion):
    """Imprime PASA/FALLA para una comprobación y lleva la cuenta."""
    global _passed, _failed
    if condicion:
        _passed += 1
        print(f"  [PASA]  {nombre}")
    else:
        _failed += 1
        print(f"  [FALLA] {nombre}")


# --------------------------------------------------------------------------- #
#  Ayudas para fabricar velas de prueba
# --------------------------------------------------------------------------- #
def vela(open_, high, low, close, vol, broker_time=None):
    c = {"open": open_, "high": high, "low": low, "close": close, "tickVolume": vol,
         "time": "2026-06-24T00:00:00.000Z"}
    if broker_time:
        c["brokerTime"] = broker_time
    return c


def vela_hora(hora, high, low, close, vol, fecha="2026-06-24"):
    """Vela con brokerTime, para la estrategia asiática (que usa la hora)."""
    return vela(low, high, low, close, vol, f"{fecha} {hora:02d}:00:00.000")


# --------------------------------------------------------------------------- #
#  PRUEBAS: Asian Range Breakout
# --------------------------------------------------------------------------- #
def test_asian():
    print("\n== Estrategia: Asian Range Breakout ==")
    params = {
        "asian_start_hour": 0, "asian_end_hour": 7,
        "london_start_hour": 8, "london_end_hour": 10,
        "ny_start_hour": 15, "ny_end_hour": 17,
        "trade_sessions_only": True,
        "breakout_buffer_pips": 2.0, "volume_lookback": 5,
        "volume_surge_multiplier": 2.0, "atr_period": 5,
        "atr_sl_floor_mult": 1.0, "tp_rr_multiplier": 2.0,
    }

    # Sesión asiática 0-6h: rango 1.1000 - 1.1050.
    base = [vela_hora(h, 1.1050, 1.1000, 1.1025, 100) for h in range(0, 7)]
    # Velas 7-8h: tranquilas, volumen normal.
    base += [vela_hora(h, 1.1040, 1.1010, 1.1030, 100) for h in range(7, 9)]

    # Caso 1: ruptura ALCISTA en Londres (9h) con volumen alto -> COMPRA.
    velas = base + [vela_hora(9, 1.1090, 1.1050, 1.1080, 500)]
    d, motivo, niveles = strategy_asian.decide(
        velas, params, server_hour=9, pip_size=0.0001
    )
    check("ruptura alcista en sesión -> 'buy'", d == "buy")
    check("devuelve niveles para SL/TP", isinstance(niveles, dict) and "range_high" in niveles)
    print(f"         (motivo: {motivo})")

    # Caso 2: ruptura BAJISTA en NY (15h) con volumen alto -> VENTA.
    base_ny = [vela_hora(h, 1.1050, 1.1000, 1.1025, 100) for h in range(0, 7)]
    base_ny += [vela_hora(h, 1.1040, 1.1010, 1.1030, 100) for h in range(7, 15)]
    velas = base_ny + [vela_hora(15, 1.0950, 1.0910, 1.0920, 500)]
    d, motivo, _ = strategy_asian.decide(velas, params, server_hour=15, pip_size=0.0001)
    check("ruptura bajista en sesión -> 'sell'", d == "sell")
    print(f"         (motivo: {motivo})")

    # Caso 3: sin ruptura (precio dentro del rango) -> NO opera.
    velas = base + [vela_hora(9, 1.1040, 1.1015, 1.1030, 500)]
    d, motivo, _ = strategy_asian.decide(velas, params, server_hour=9, pip_size=0.0001)
    check("precio dentro del rango -> None (no opera)", d is None)
    print(f"         (motivo: {motivo})")

    # Caso 4: hay ruptura pero FUERA de sesión (12h) -> NO opera.
    velas = base + [vela_hora(12, 1.1090, 1.1050, 1.1080, 500)]
    d, motivo, _ = strategy_asian.decide(velas, params, server_hour=12, pip_size=0.0001)
    check("ruptura fuera de sesión -> None (no opera)", d is None)
    print(f"         (motivo: {motivo})")

    # Caso 5: ruptura pero SIN volumen suficiente -> NO opera.
    velas = base + [vela_hora(9, 1.1090, 1.1050, 1.1080, 100)]
    d, motivo, _ = strategy_asian.decide(velas, params, server_hour=9, pip_size=0.0001)
    check("ruptura sin volumen -> None (no opera)", d is None)
    print(f"         (motivo: {motivo})")


# --------------------------------------------------------------------------- #
#  PRUEBAS: Multi-TF Order Flow
# --------------------------------------------------------------------------- #
def test_multitf():
    print("\n== Estrategia: Multi-TF Order Flow ==")
    params = {
        "ema_trend_period": 50,
        "of_volume_multiplier": 1.5,
        "of_min_body_ratio": 0.60,
        "of_volume_sma_period": 20,
    }

    # Tendencia ALCISTA: cierres en aumento en D1 y H4 (el precio queda por
    # encima de la EMA en los 3 timeframes).
    d1 = [vela(1.0 + i * 0.01, 1.0 + i * 0.01 + 0.005, 1.0 + i * 0.01 - 0.005,
               1.0 + i * 0.01, 100) for i in range(60)]
    h4 = [vela(1.0 + i * 0.005, 1.0 + i * 0.005 + 0.003, 1.0 + i * 0.005 - 0.003,
               1.0 + i * 0.005, 100) for i in range(120)]
    # M5: velas planas + última vela alcista con volumen alto y cuerpo amplio.
    m5 = [vela(1.30, 1.301, 1.299, 1.30, 100) for _ in range(25)]
    m5.append(vela(1.300, 1.361, 1.299, 1.360, 400))  # alcista, volumen x4, cuerpo amplio

    d, motivo = strategy_multitf.decide(d1, h4, m5, params)
    check("tendencia alcista + order flow alcista -> 'buy'", d == "buy")
    print(f"         (motivo: {motivo})")

    # Tendencia BAJISTA: cierres en descenso + última M5 bajista fuerte.
    d1b = [vela(2.0 - i * 0.01, 2.0 - i * 0.01 + 0.005, 2.0 - i * 0.01 - 0.005,
                2.0 - i * 0.01, 100) for i in range(60)]
    h4b = [vela(2.0 - i * 0.005, 2.0 - i * 0.005 + 0.003, 2.0 - i * 0.005 - 0.003,
                2.0 - i * 0.005, 100) for i in range(120)]
    m5b = [vela(1.30, 1.301, 1.299, 1.30, 100) for _ in range(25)]
    m5b.append(vela(1.360, 1.361, 1.299, 1.300, 400))  # bajista, volumen x4

    d, motivo = strategy_multitf.decide(d1b, h4b, m5b, params)
    check("tendencia bajista + order flow bajista -> 'sell'", d == "sell")
    print(f"         (motivo: {motivo})")

    # Tendencia NO alineada (plana) -> NO opera.
    plano = [vela(1.30, 1.301, 1.299, 1.30, 100) for _ in range(60)]
    d, motivo = strategy_multitf.decide(plano, plano, plano, params)
    check("tendencia plana -> None (no opera)", d is None)
    print(f"         (motivo: {motivo})")


if __name__ == "__main__":
    print("Probando el cerebro de las estrategias (sin conectar a ningún broker)...")
    test_asian()
    test_multitf()

    print(f"\nResultado: {_passed} pasaron, {_failed} fallaron.")
    if _failed == 0:
        print("TODAS LAS PRUEBAS PASARON - las estrategias deciden correctamente.")
        sys.exit(0)
    else:
        print("Hay pruebas que fallaron. Revisa el detalle de arriba.")
        sys.exit(1)
