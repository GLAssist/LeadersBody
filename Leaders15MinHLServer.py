from cProfile import label
import asyncio, json, os
import os
import requests
import websockets
import numpy as np
from datetime import date, timedelta, time as dtime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
load_dotenv()

IST = ZoneInfo("Asia/Kolkata")
CLIENTS = set()
LATEST = None          # last payload, sent to new clients on connect

#  Loading Environment
ENCTOKEN = os.getenv("ENCTOKEN")
USER = os.getenv("USER_ID")

# Load Scripts 
scripts = json.load(open("scripts.json", "r", encoding="utf-8"))


def GetScriptOHLC(instrument='256265', timeframe=15, timeunit='minute', ):

    today = date.today().strftime('%Y-%m-%d')
    fromday = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
    url = f"https://kite.zerodha.com/oms/instruments/historical/{instrument}/{timeframe}{timeunit}?user_id={USER}&oi=1&from={fromday}&to={today}"
    headers = {
        "Authorization": f"enctoken {ENCTOKEN}"
    }
    try:
        # Make a GET request
        response = requests.get(url, headers=headers, timeout=10)  # timeout in seconds
        
        # Raise an error for bad status codes (4xx, 5xx)
        response.raise_for_status()
        
        # If the API returns JSON, parse it
        data = response.json()
        candles = data.get('data').get('candles')
        return candles
    
    except requests.exceptions.Timeout:
        print("Request timed out.")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")

def GetCandleAvgBody(matrix):

    if not matrix:
        return 0

    close = matrix[-1][4]

    highs = np.array([x[2] for x in matrix[:-1]])
    lows = np.array([x[3] for x in matrix[:-1]])

    unqhighs = np.unique(highs)
    unqlows = np.unique(lows)

    if len(unqhighs) < 2 or len(unqlows) < 2:
        return 0

    maxOfHigh = unqhighs[-1]
    secondMaxOfHigh = np.average(unqhighs)
    minOfHigh = unqhighs[0]

    maxOfLow = unqlows[-1]
    secondLowOfLow = np.average(unqlows)
    minOfLow = unqlows[0]

    levels = [
        maxOfHigh,
        secondMaxOfHigh,
        minOfHigh,
        maxOfLow,
        secondLowOfLow,
        minOfLow
    ]

    # Remove crossing inner levels
    while len(levels) > 2:
        mid = len(levels) // 2

        if levels[mid - 1] <= levels[mid]:
            del levels[mid - 1:mid + 1]
        else:
            break

    counter = sum(close > price for price in levels)

    return counter/len(levels)

def ScriptsOHLC():
    scriptHL = {}
    for item in scripts:
        scriptCandles = GetScriptOHLC(item["instrument_token"])
        sentiment = GetCandleAvgBody(scriptCandles)
        scriptHL[item["tradingsymbol"]] = sentiment

    print(scriptHL)       
    return scriptHL


URL = os.getenv("WS_URL", "ws://localhost:7000")

async def run_sender():
    while True:                                          # reconnect loop
        try:
            async with websockets.connect(URL) as ws:
                print("connected")
                while True:
                    OHLC = await asyncio.to_thread(ScriptsOHLC)   # blocking requests -> thread
                    if OHLC:
                        await ws.send(json.dumps({'OHLC': OHLC}))
                        print("sent", len(OHLC), "candles")
                    else:
                        print("no data")
                    await asyncio.sleep(300)             # 5 minutes
        except (OSError, websockets.ConnectionClosed) as e:
            print("connection lost:", e, "- retrying in 5s")
            await asyncio.sleep(5)

asyncio.run(run_sender())