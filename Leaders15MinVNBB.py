from cProfile import label
from urllib.parse import urlparse
from enum import Enum
import asyncio, json, os
import requests
import websockets
import numpy as np
from collections import defaultdict, deque
from datetime import date, datetime, timedelta, time as dtime
from zoneinfo import ZoneInfo
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from dotenv import load_dotenv
from websockets.headers import T
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
# Calculate the next 11:00 AM
now = datetime.now(IST)
start_time = now.replace(hour=10, minute=10, second=0, microsecond=0)

stream_data = defaultdict(list)

class ExecutionMode(Enum):
    STREAMING = 1
    SCHEDULED = 2

execution_mode = ExecutionMode.STREAMING


def GetScriptOHLC(instrument='256265', timeframe=15, timeunit='minute', ):

    today = date.today().strftime('%Y-%m-%d')
    fromday = (date.today() - timedelta(days=5)).strftime('%Y-%m-%d')
    url = f"https://kite.zerodha.com/oms/instruments/historical/{instrument}/{timeframe}{timeunit}?user_id={USER}&oi=1&from={today}&to={today}"
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
        candles = data.get('data', {}).get('candles', [])
        return candles
    
    except requests.exceptions.Timeout:
        print("Request timed out.")
    except requests.exceptions.HTTPError as err:
        print(f"HTTP error occurred: {err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")

def GetCandleAvgBody(matrix):
    if not matrix or len(matrix) < 4:
        return ['N', 'N']

    sentimentarr = []

    highs = np.array([x[2] for x in matrix[-4:-1]])
    lows = np.array([x[3] for x in matrix[-4:-1]])
    
    for idx in range(1, 3):
        if ((highs[idx]>highs[idx-1]) and (lows[idx]>lows[idx-1])):
            sentimentarr.append('U')  # U
        elif ((highs[idx]<highs[idx-1]) and (lows[idx]<lows[idx-1])):
             sentimentarr.append('D')  # D
        elif ((highs[idx]>highs[idx-1]) and (lows[idx]<lows[idx-1])):
            sentimentarr.append('V')  # V
        elif ((highs[idx]<highs[idx-1]) and (lows[idx]>lows[idx-1])):
            sentimentarr.append('N') # N
        else:
            sentimentarr.append('N')

    return sentimentarr

def ScriptsOHLC():
    martix = defaultdict(list)
    for item in scripts:
        scriptCandles = GetScriptOHLC(item["instrument_token"])
        sentiment = GetCandleAvgBody(scriptCandles)
        martix[item["tradingsymbol"]] = sentiment

    print(martix)       
    return martix

def SentimentMatrix():
    matrix = defaultdict(list)

    for symbol, prices in stream_data.items():
        num_groups = len(prices) // 3

        if num_groups > 4:
            max_price = prices.max()
            min_price = prices.min()

            rel_position = round(
                (prices[-1] - min_price) /
                (max_price - min_price) * 100, 2
            )

            three_slices = prices[:3 * num_groups].reshape(3, num_groups)

            highs = three_slices.max(axis=1)
            lows = three_slices.min(axis=1)

            for i in (1, 2):
                hh = highs[i] > highs[i-1]
                lh = lows[i] > lows[i-1]

                if hh and lh:
                    matrix[symbol].append('U')
                elif not hh and not lh:
                    matrix[symbol].append('D')
                elif not hh and lh:
                    matrix[symbol].append('N')
                elif hh and not lh:
                    matrix[symbol].append('V')

            matrix[symbol].append(rel_position)

    return matrix


async def handler(websocket):
    global execution_mode
    print("Client connected")

    try:
        async for message in websocket:

            # Ignore incoming tick processing in scheduled mode
            if execution_mode != ExecutionMode.STREAMING:
                continue

            data = json.loads(message)
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

                sentiment = SentimentMatrix()   # blocking requests -> thread
                if sentiment:
                    await websocket.send(json.dumps({'LeadersSentiment': sentiment}))
                    print(f"sent from message: {len(sentiment)}")
                else:
                    print("no data")
    except websockets.ConnectionClosed:
        print("Client disconnected")


async def scheduled_broadcast(websocket):
    global execution_mode

    if execution_mode == ExecutionMode.SCHEDULED:
        return

    try:
        # Stop live sentiment processing
        execution_mode = ExecutionMode.SCHEDULED

        print("Switching to SCHEDULED mode")       

        sentiment = await asyncio.to_thread(ScriptsOHLC)
        if sentiment:
            await websocket.send(json.dumps({'LeadersSentiment': sentiment}))
            print(f"Sent feom schedule: {len(sentiment)}")
    except websockets.ConnectionClosed:
        print("WebSocket disconnected during scheduled broadcast")

    except Exception as e:
        print(f"Scheduled broadcast error: {e}")           


URL = os.getenv("WS_URL", f"ws://{SERVER}:7000")

async def check_socket(url, timeout=3):
    parsed = urlparse(url)
    host = parsed.hostname
    port = parsed.port or (443 if parsed.scheme == "wss" else 80)

    try:
        # Attempt to open a raw TCP socket connection
        reader, writer = await asyncio.wait_for(
            asyncio.open_connection(host, port),
            timeout=timeout
        )
        # Close the test socket immediately
        writer.close()
        await writer.wait_closed()
        return True
    except (OSError, asyncio.TimeoutError):
        return False

async def run_sender():

    if await check_socket(URL):  
        print("Socket is open! Connecting to WebSocket...")  
        async with websockets.connect(URL, subprotocols=['LeadersSentimentProvider_Receiver']) as ws:
            print("Connected!")
            try:
                # This keeps the connection alive and listens for incoming messages
                await handler(ws)
            finally:
                pass
            await asyncio.Future()  # Keeps the server running forever


asyncio.run(run_sender())