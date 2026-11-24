from cProfile import label
import asyncio, json, os
import os
import requests
import websockets
import numpy as np
from collections import defaultdict, deque
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
SERVER = os.getenv("SERVER")
# Load Scripts 
scripts = json.load(open("scripts.json", "r", encoding="utf-8"))

stream_data = defaultdict(list)


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

def SentimentMatrix():
    martix = defaultdict(list)
    for symbol, prices in stream_data.items():
        num_groups = len(prices) // 3  # Size of each of the 3 slices
        
        # Triggers once each of the 3 slices has at least 5 elements (length >= 15)
        if num_groups > 4:
            # 1. Slice from the left to keep only 3 * num_groups elements
            left_slice = np.array(prices[:3 * num_groups])
            
            # 2. Reshape into 3 rows, each of length 'num_groups'
            three_slices = left_slice.reshape(3, num_groups)
            
            # 3. Get High (max) and Low (min) for each of the 3 slices
            slice_highs = three_slices.max(axis=1)
            slice_lows = three_slices.min(axis=1)
            
            # print(f"--- {symbol} (3 slices of size {num_groups}) ---")
            for i in range(1, 3):
                if ((slice_highs[i] > slice_highs[i-1]) and (slice_lows[i] > slice_lows[i-1])):
                    martix[symbol].append('U')
                if ((slice_highs[i] < slice_highs[i-1]) and (slice_lows[i] < slice_lows[i-1])):
                    martix[symbol].append('D')
                if ((slice_highs[i] < slice_highs[i-1]) and (slice_lows[i] > slice_lows[i-1])):
                    martix[symbol].append('N')
                if ((slice_highs[i] > slice_highs[i-1]) and (slice_lows[i] < slice_lows[i-1])):
                    martix[symbol].append('V') 

    return martix

URL = os.getenv("WS_URL", f"ws://{SERVER}:7000")


async def handler(websocket):
    print("Client connected")

    try:
        async for message in websocket:
            data = data = json.loads(message)
            hlc_data = data.get("HLC")
            if isinstance(hlc_data, dict):
                for symbol, values in hlc_data.items():
                    close_price = values.get("Close")
                    
                    if close_price is not None:
                        if symbol not in stream_data:
                            print(f"New symbol added: {symbol}")
                            
                        stream_data[symbol].append(close_price)
                        if len(stream_data[symbol]) > 1200:
                            stream_data[symbol].pop(0)

                sentiment = await asyncio.to_thread(SentimentMatrix)   # blocking requests -> thread
                if sentiment:
                    await websocket.send(json.dumps({'LeadersSentiment': sentiment}))
                    print("sent", len(sentiment), sentiment)
                else:
                    print("no data")
    except websockets.ConnectionClosed:
        print("Client disconnected")




async def run_sender():
    async with websockets.connect(URL, subprotocols=['LeadersSentimentProvider_Receiver']) as ws:
        print(f"Server listening on {URL}")
        await handler(ws)  # Call your handler function here
        await asyncio.Future()  # Keeps the server running forever


asyncio.run(run_sender())