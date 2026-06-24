"""
open_one.py
-----------
Herramienta de prueba: abre UNA orden de mercado directamente con el SDK de
MetaApi (sin pasar por el panel). Sirve para confirmar que se pueden mandar
operaciones a una cuenta concreta.

Uso (PowerShell):
    $env:METAAPI_TOKEN      = "tu-token"
    $env:METAAPI_ACCOUNT_ID = "el-account-id"
    python open_one.py               # EURUSD buy 0.01 por defecto
    python open_one.py EURUSD sell 0.02
"""

import asyncio
import os
import sys

from metaapi_cloud_sdk import MetaApi

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

TOKEN = os.getenv("METAAPI_TOKEN", "")
ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")

SYMBOL = sys.argv[1] if len(sys.argv) > 1 else "EURUSD"
SIDE = (sys.argv[2] if len(sys.argv) > 2 else "buy").lower()
VOLUME = float(sys.argv[3]) if len(sys.argv) > 3 else 0.01


async def main():
    if not TOKEN or not ACCOUNT_ID:
        raise SystemExit("Define METAAPI_TOKEN y METAAPI_ACCOUNT_ID en el entorno o .env.")

    print(f"Cuenta: {ACCOUNT_ID}")
    print(f"Orden a probar: {SIDE.upper()} {VOLUME} {SYMBOL}")

    api = MetaApi(TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

    print(f"Estado de la cuenta: {account.state} / {account.connection_status}")
    if account.state != "DEPLOYED":
        print("Desplegando cuenta...")
        await account.deploy()
    print("Esperando conexión al broker...")
    await account.wait_connected()

    connection = account.get_rpc_connection()
    await connection.connect()
    try:
        await connection.wait_synchronized(60)

        price = await connection.get_symbol_price(SYMBOL)
        print(f"Precio {SYMBOL}: bid={price.get('bid')} ask={price.get('ask')}")

        options = {"comment": "EasTest"}
        if SIDE == "sell":
            result = await connection.create_market_sell_order(SYMBOL, VOLUME, None, None, options)
        else:
            result = await connection.create_market_buy_order(SYMBOL, VOLUME, None, None, options)

        print("RESULTADO:", result)
        code = result.get("stringCode") if isinstance(result, dict) else None
        if code == "TRADE_RETCODE_DONE":
            print("OK: la operación se abrió correctamente.")
        else:
            print(f"Atención: el broker respondió {code}. Revisa el detalle arriba.")
    finally:
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
