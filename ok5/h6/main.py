import os
import time
import math
import json
import logging
import pandas as pd
import requests
import threading
import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from pydantic import BaseModel
from ta.trend import MACD
from ta.momentum import StochRSIIndicator
from binance.client import Client
from binance import ThreadedWebsocketManager
from dotenv import load_dotenv
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor

# ================= TẢI BIẾN MÔI TRƯỜNG =================
load_dotenv()

API_KEY = os.getenv('BINANCE_API_KEY')
API_SECRET = os.getenv('BINANCE_API_SECRET')
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

KILL_SWITCH_PASSWORD = os.getenv('KILL_SWITCH_PASSWORD', 'admin123')

SYMBOL = 'ETHUSDT'
LEVERAGE = 4
MARGIN_TYPE = 'ISOLATED'
QUANTITY_PRECISION = 3
STOP_LOSS_PCT = 0.1 

STATE_FILE = "bot_state.json"
LOG_FILE = "bot.log"

# ================= CẤU HÌNH LOGGING RA FILE =================
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        RotatingFileHandler(LOG_FILE, maxBytes=10*1024*1024, backupCount=5, encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Chạy thử nghiệm (Testnet = True) - Đổi thành False nếu trade thật
client = Client(API_KEY, API_SECRET, testnet=True)

# ================= BIẾN TRẠNG THÁI & THREAD LOCKS =================
state_lock = threading.Lock()
data_lock = threading.Lock()

bot_state = {
    "logs": [],
    "total_trades": 0,
    "winning_trades": 0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
    "total_pnl": 0.0,
    "entry_fee": 0.0,
    "current_position": "NONE",
    "position_amt": 0.0,
    "entry_price": 0.0,
    "is_running": False,
    "kill_switch": False,
    "latest_indicators": {} 
}

live_data = {
    '1h': pd.DataFrame(),
    '6h': pd.DataFrame(),
    '1w': pd.DataFrame()
}

bot_thread = None
ws_manager = None
last_ws_update_time = time.time()
current_bnb_price = 600.0  # Biến toàn cục lưu giá BNB

telegram_executor = ThreadPoolExecutor(max_workers=3)

# ================= HÀM HỖ TRỢ & STATE =================
def custom_log(message):
    logging.info(message)
    with state_lock:
        timestamp = time.strftime('%H:%M:%S')
        bot_state["logs"].append(f"[{timestamp}] {message}")
        if len(bot_state["logs"]) > 300:
            bot_state["logs"].pop(0)

def load_state():
    global bot_state
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, 'r', encoding='utf-8') as f:
                saved_state = json.load(f)
            with state_lock:
                bot_state.update(saved_state)
            bot_state["kill_switch"] = False 
            custom_log("✅ Đã khôi phục trạng thái bot_state.json!")
        except Exception as e:
            custom_log(f"⚠️ Lỗi đọc file state: {e}")

def save_state_loop():
    while True:
        try:
            with state_lock:
                state_copy = bot_state.copy()
                state_copy["logs"] = list(bot_state["logs"])
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(state_copy, f, indent=4)
        except Exception as e:
            custom_log(f"⚠️ Lỗi ghi file state: {e}")
        time.sleep(10)

def send_telegram_notification(message):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID: return
    
    def _send():
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {'chat_id': TELEGRAM_CHAT_ID, 'text': message, 'parse_mode': 'HTML'}
        try:
            requests.post(url, data=payload, timeout=5)
        except Exception as e:
            custom_log(f"⚠️ Lỗi gửi Telegram: {e}")
            
    telegram_executor.submit(_send)

# ================= LUỒNG CẬP NHẬT GIÁ BNB (FIX BOTTLENECK) =================
def update_bnb_price_loop():
    global current_bnb_price
    while True:
        try:
            ticker = safe_api_call(client.futures_symbol_ticker, symbol="BNBUSDT")
            if ticker and 'price' in ticker:
                with state_lock:
                    current_bnb_price = float(ticker['price'])
        except Exception as e:
            pass
        time.sleep(60) # Cập nhật mỗi 60 giây

# ================= RETRY API & ORDER FILL CHECK =================
def safe_api_call(func, retries=3, delay=1.5, *args, **kwargs):
    for attempt in range(retries):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            if "-4046" in str(e): return None 
            custom_log(f"⚠️ Lỗi API (Lần {attempt+1}/{retries}): {e}")
            if attempt < retries - 1:
                time.sleep(delay)
            else:
                custom_log(f"API thất bại hoàn toàn sau {retries} lần thử: {e}")
                return None 

def check_order_filled(order_id, max_retries=10, delay=1.0):
    for _ in range(max_retries):
        try:
            order_info = client.futures_get_order(symbol=SYMBOL, orderId=order_id)
            if order_info['status'] == 'FILLED':
                return True
            if order_info['status'] in ['CANCELED', 'EXPIRED', 'REJECTED']:
                custom_log(f"⚠️ Lệnh {order_id} bị hủy/từ chối. Trạng thái: {order_info['status']}")
                return False
        except Exception:
            pass
        time.sleep(delay)
    custom_log(f"⚠️ Timeout khi chờ lệnh {order_id} khớp hoàn toàn!")
    return False

# ================= WEBSOCKET REAL-TIME DATA =================
def init_historical_data():
    custom_log("Đang tải dữ liệu lịch sử klines...")
    intervals = {'1h': Client.KLINE_INTERVAL_1HOUR, '6h': Client.KLINE_INTERVAL_6HOUR, '1w': Client.KLINE_INTERVAL_1WEEK}
    
    with data_lock:
        for tf, interval in intervals.items():
            klines = safe_api_call(client.futures_klines, symbol=SYMBOL, interval=interval, limit=1500)
            if klines is None: continue
            df = pd.DataFrame(klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_av', 'trades', 'tb_base_av', 'tb_quote_av', 'ignore'])
            for col in ['open', 'high', 'low', 'close', 'volume']:
                df[col] = df[col].astype(float)
            live_data[tf] = df

def process_ws_kline(msg, tf):
    global last_ws_update_time
    if 'k' not in msg: return
    
    last_ws_update_time = time.time()
    kline = msg['k']
    new_row = {
        'timestamp': kline['t'], 'open': float(kline['o']), 'high': float(kline['h']), 
        'low': float(kline['l']), 'close': float(kline['c']), 'volume': float(kline['v']), 
        'close_time': kline['T']
    }
    
    with data_lock:
        df = live_data[tf]
        if df.empty: return
        if df.iloc[-1]['timestamp'] == new_row['timestamp']:
            for col in new_row:
                df.at[df.index[-1], col] = new_row[col]
        else:
            df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
            # FIX LỖI HAO HỤT: Giữ lại đúng 1500 nến để đảm bảo dữ liệu cho MACD/StochRSI
            if len(df) > 1500: 
                df = df.iloc[-1500:].reset_index(drop=True)
            live_data[tf] = df

def process_user_data(msg):
    event_type = msg.get('e')
    if event_type == 'ACCOUNT_UPDATE':
        positions = msg.get('a', {}).get('P', [])
        for p in positions:
            if p['s'] == SYMBOL:
                with state_lock:
                    bot_state["position_amt"] = float(p['pa'])
                    bot_state["entry_price"] = float(p['ep'])
                    
                    if bot_state["position_amt"] > 0:
                        bot_state["current_position"] = "LONG"
                    elif bot_state["position_amt"] < 0:
                        bot_state["current_position"] = "SHORT"
                    else:
                        bot_state["current_position"] = "NONE"

def start_websockets():
    global ws_manager, last_ws_update_time
    
    if ws_manager:
        try:
            ws_manager.stop()
            ws_manager.join()
        except: pass

    # Testnet = True (Đổi thành False nếu chạy tiền thật)
    ws_manager = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET, testnet=True)
    ws_manager.start()
    
    ws_manager.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '1h'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1HOUR)
    ws_manager.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '6h'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_6HOUR)
    ws_manager.start_kline_futures_socket(callback=lambda m: process_ws_kline(m, '1w'), symbol=SYMBOL, interval=Client.KLINE_INTERVAL_1WEEK)
    
    ws_manager.start_futures_user_socket(callback=process_user_data)

    last_ws_update_time = time.time()
    custom_log("✅ Đã kết nối WebSocket & User Data Stream thành công!")

# ================= WATCHDOG =================
def ws_watchdog_loop():
    global last_ws_update_time
    while True:
        time.sleep(20)
        if time.time() - last_ws_update_time > 60:
            custom_log("⚠️ CẢNH BÁO: Mất kết nối WebSocket (> 60s). Đang Resync...")
            try:
                init_historical_data()
                start_websockets()
            except Exception as e:
                custom_log(f"⚠️ Lỗi khi Resync WebSocket: {e}")
            last_ws_update_time = time.time()

# ================= CHỈ BÁO & GIAO DỊCH =================
def get_realtime_indicators():
    inds = {}
    with data_lock:
        for tf in ['1h', '6h', '1w']:
            df = live_data[tf].copy()
            if df.empty or len(df) < 8: return None

            macd = MACD(close=df['close'])
            stoch_rsi = StochRSIIndicator(close=df['close'])
            
            inds[tf] = {
                'macd': macd.macd_diff().iloc[-1], 'macd_prev': macd.macd_diff().iloc[-2], 'macd_prev2': macd.macd_diff().iloc[-3],
                'k': stoch_rsi.stochrsi_k().iloc[-1], 'd': stoch_rsi.stochrsi_d().iloc[-1],
                'k_prev': stoch_rsi.stochrsi_k().iloc[-2], 'd_prev': stoch_rsi.stochrsi_d().iloc[-2],
                'highest_7': df['high'].iloc[-8:-1].max(),
                'lowest_7': df['low'].iloc[-8:-1].min(),
            }
    return inds

def execute_trade(side, price, reason, ind_data, is_closing=False, amt_to_close=0):
    try:
        if is_closing:
            qty = abs(amt_to_close)
            order_side = 'SELL' if side == 'LONG' else 'BUY'
            custom_log(f"⏳ ĐANG ĐÓNG {side} - Đợi khớp lệnh...")
            order = safe_api_call(client.futures_create_order, symbol=SYMBOL, side=order_side, type='MARKET', quantity=qty, reduceOnly=True)
        else:
            account_info = safe_api_call(client.futures_account)
            if not account_info: 
                custom_log("⚠️ Không lấy được số dư. Bỏ qua lệnh.")
                return
                
            available_usdt = float(account_info['availableBalance'])
            trade_margin = available_usdt * 0.95 
            if trade_margin < 5:
                custom_log("Số dư quá thấp!")
                return
            qty = math.floor((trade_margin * LEVERAGE / price) * (10**QUANTITY_PRECISION)) / (10**QUANTITY_PRECISION)
            order_side = 'BUY' if side == 'LONG' else 'SELL'
            custom_log(f"🚀 ĐANG MỞ {side} - Đợi khớp lệnh...")
            order = safe_api_call(client.futures_create_order, symbol=SYMBOL, side=order_side, type='MARKET', quantity=qty)

        if not order or 'orderId' not in order:
            custom_log("⚠️ Không tạo được lệnh.")
            return

        is_filled = check_order_filled(order['orderId'])
        if not is_filled:
            custom_log("⚠️ Lệnh không fill hoàn toàn!")

        # ================= FIX LỖI NHỒI LỆNH =================
        # KHÓA TRẠNG THÁI NGAY LẬP TỨC TRƯỚC KHI LÀM BẤT CỨ VIỆC GÌ KHÁC
        with state_lock:
            if is_closing:
                bot_state["current_position"] = "NONE"
                bot_state["position_amt"] = 0.0
            else:
                bot_state["current_position"] = side
                bot_state["position_amt"] = qty if side == 'LONG' else -qty
                bot_state["entry_price"] = price
        # =====================================================

        # Lấy thông tin trade để tính PnL và Phí
        trades = safe_api_call(client.futures_account_trades, symbol=SYMBOL, orderId=order['orderId'])
        if not trades: 
            custom_log("⚠️ Chốt lệnh thành công nhưng không lấy được log trades để tính phí/PnL lúc này.")
            if is_closing:
                send_telegram_notification(f"🔴 <b>ĐÓNG {side}</b>\nLý do: {reason}\n(Lỗi lấy thông tin PnL từ API)")
            else:
                send_telegram_notification(f"🟢 <b>MỞ {side}</b>\nGiá: {price}\nKhối lượng: {qty}\nLý do: {reason}")
            return
        
        fee_usdt = 0.0
        for t in trades:
            commission = float(t['commission'])
            commission_asset = t['commissionAsset']
            if commission_asset == 'USDT':
                fee_usdt += commission
            elif commission_asset == 'BNB':
                with state_lock:
                    cached_bnb_price = current_bnb_price
                fee_usdt += (commission * cached_bnb_price)
        
        if is_closing:
            pnl = sum(float(t['realizedPnl']) for t in trades)
            with state_lock:
                entry_fee = bot_state.get("entry_fee", 0.0)
                net_pnl = pnl - fee_usdt - entry_fee
                bot_state["total_trades"] += 1
                bot_state["total_pnl"] += net_pnl
                bot_state["entry_fee"] = 0.0 
                if net_pnl > 0: 
                    bot_state["winning_trades"] += 1
                    bot_state["gross_profit"] += net_pnl
                else: 
                    bot_state["gross_loss"] += abs(net_pnl)

            custom_log(f"🏁 ĐÃ ĐÓNG {side} | Thực nhận: {net_pnl:.4f} USDT")
            send_telegram_notification(f"🔴 <b>ĐÓNG {side}</b>\nLợi nhuận: {net_pnl:.4f} USDT\nLý do: {reason}")
        else:
            with state_lock:
                bot_state["entry_fee"] = fee_usdt

            custom_log(f"✅ ĐÃ MỞ {side} | KL: {qty} | Phí: {fee_usdt:.4f} USDT")
            send_telegram_notification(f"🟢 <b>MỞ {side}</b>\nGiá: {price}\nKhối lượng: {qty}\nLý do: {reason}")

    except Exception as e:
        custom_log(f"⚠️ Lỗi thực thi lệnh: {e}")

def run_bot():
    custom_log("Thiết lập đòn bẩy và margin...")
    safe_api_call(client.futures_change_leverage, symbol=SYMBOL, leverage=LEVERAGE)
    
    try: 
        client.futures_change_margin_type(symbol=SYMBOL, marginType=MARGIN_TYPE)
    except Exception as e: 
        if "-4046" not in str(e):
            custom_log(f"⚠️ Lỗi set Margin Type: {e}")

    pos_info = safe_api_call(client.futures_position_information, symbol=SYMBOL)
    if pos_info:
        p = next((x for x in pos_info if x['symbol'] == SYMBOL), None)
        if p is not None:
            with state_lock:
                bot_state["position_amt"] = float(p['positionAmt'])
                bot_state["entry_price"] = float(p['entryPrice'])
                if bot_state["position_amt"] > 0:
                    bot_state["current_position"] = "LONG"
                elif bot_state["position_amt"] < 0:
                    bot_state["current_position"] = "SHORT"
                else:
                    bot_state["current_position"] = "NONE"

    init_historical_data()
    start_websockets()
    send_telegram_notification(f"🚀 <b>BOT ĐÃ KHỞI ĐỘNG</b>\nĐang theo dõi {SYMBOL}...")

    while True:
        try:
            if bot_state["kill_switch"]:
                time.sleep(5)
                continue

            inds = get_realtime_indicators()
            if not inds: 
                time.sleep(1)
                continue

            # Chuyển đổi format indicator để cache cho Frontend Dashboard
            formatted_inds = {}
            for tf, d in inds.items():
                formatted_inds[tf] = {
                    "macd_t0": float(d['macd']), "macd_t1": float(d['macd_prev']), "macd_t2": float(d['macd_prev2']),
                    "stoch_k_t0": float(d['k']), "stoch_k_t1": float(d['k_prev']),
                    "stoch_d_t0": float(d['d']), "stoch_d_t1": float(d['d_prev'])
                }
            with state_lock:
                bot_state["latest_indicators"] = formatted_inds

            current_price = float(live_data['1h']['close'].iloc[-1])
            
            with state_lock:
                amt = bot_state["position_amt"]
                entry_price = bot_state["entry_price"]
                current_pos = bot_state["current_position"]

            if current_pos != "NONE":
                is_long = current_pos == "LONG"
                is_short = current_pos == "SHORT"
                
                if (is_long and current_price <= entry_price * (1 - STOP_LOSS_PCT)) or \
                   (is_short and current_price >= entry_price * (1 + STOP_LOSS_PCT)):
                    execute_trade(current_pos, current_price, f"CẮT LỖ CỨNG {STOP_LOSS_PCT*100}%", inds, is_closing=True, amt_to_close=amt)
                    continue

                b_1w = (inds['1w']['macd'] > inds['1w']['macd_prev']) and (inds['1w']['macd'] >= 0 or inds['1w']['k'] > inds['1w']['d'])
                b_6h = (inds['6h']['macd'] > inds['6h']['macd_prev']) and (inds['6h']['macd'] >= 0 or inds['6h']['k'] > inds['6h']['d'])
                s_1w = (inds['1w']['macd'] < inds['1w']['macd_prev']) and (inds['1w']['macd'] <= 0 or inds['1w']['k'] < inds['1w']['d'])
                s_6h = (inds['6h']['macd'] < inds['6h']['macd_prev']) and (inds['6h']['macd'] <= 0 or inds['6h']['k'] < inds['6h']['d'])

                if is_long and not (b_1w and b_6h):
                    if current_price <= entry_price and (inds['1h']['lowest_7'] and current_price < inds['1h']['lowest_7']):
                        execute_trade("LONG", current_price, "CẮT LỖ ĐỘNG", inds, is_closing=True, amt_to_close=amt)
                    elif current_price > entry_price:
                        execute_trade("LONG", current_price, "CHỐT LỜI", inds, is_closing=True, amt_to_close=amt)

                elif is_short and not (s_1w and s_6h):
                    if current_price >= entry_price and (inds['1h']['highest_7'] and current_price > inds['1h']['highest_7']):
                        execute_trade("SHORT", current_price, "CẮT LỖ ĐỘNG", inds, is_closing=True, amt_to_close=amt)
                    elif current_price < entry_price:
                        execute_trade("SHORT", current_price, "CHỐT LỜI", inds, is_closing=True, amt_to_close=amt)

            elif current_pos == "NONE":
                c_buy_1w = (inds['1w']['macd'] > inds['1w']['macd_prev']) and (inds['1w']['macd'] >= 0 or inds['1w']['k'] > inds['1w']['d'])
                c_buy_6h = (inds['6h']['macd'] > inds['6h']['macd_prev']) and (inds['6h']['macd'] >= 0 or inds['6h']['k'] > inds['6h']['d'])
                
                # CẬP NHẬT LOGIC 1H SỬ DỤNG NẾN T0 VÀ T-1
                c_buy_1h = inds['1h']['macd'] > inds['1h']['macd_prev']

                c_sell_1w = (inds['1w']['macd'] < inds['1w']['macd_prev']) and (inds['1w']['macd'] <= 0 or inds['1w']['k'] < inds['1w']['d'])
                c_sell_6h = (inds['6h']['macd'] < inds['6h']['macd_prev']) and (inds['6h']['macd'] <= 0 or inds['6h']['k'] < inds['6h']['d'])
                
                # CẬP NHẬT LOGIC 1H SỬ DỤNG NẾN T0 VÀ T-1
                c_sell_1h = inds['1h']['macd'] < inds['1h']['macd_prev']

                if c_buy_1w and c_buy_6h and c_buy_1h:
                    execute_trade("LONG", current_price, "ĐỒNG THUẬN TĂNG", inds)
                elif c_sell_1w and c_sell_6h and c_sell_1h:
                    execute_trade("SHORT", current_price, "ĐỒNG THUẬN GIẢM", inds)

            time.sleep(1)
        except Exception as e:
            custom_log(f"⚠️ Lỗi vòng lặp chính: {e}")
            time.sleep(5)

# ================= THREAD MONITOR =================
def thread_monitor():
    global bot_thread
    while True:
        if bot_thread is None or not bot_thread.is_alive():
            custom_log("⚠️ Phát hiện Bot Thread bị dừng. Đang khởi động lại...")
            bot_thread = threading.Thread(target=run_bot, daemon=True)
            bot_thread.start()
        time.sleep(10)

# ================= FASTAPI & DASHBOARD =================
@asynccontextmanager
async def lifespan(app: FastAPI):
    load_state()
    threading.Thread(target=save_state_loop, daemon=True).start()
    threading.Thread(target=thread_monitor, daemon=True).start()
    threading.Thread(target=ws_watchdog_loop, daemon=True).start()
    threading.Thread(target=update_bnb_price_loop, daemon=True).start() # Khởi động luồng giá BNB ngầm
    yield
    try:
        with state_lock:
            bot_state["is_running"] = False
        if ws_manager: ws_manager.stop()
        telegram_executor.shutdown(wait=False)
    except: pass

app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html", context={})

@app.get("/api/stats")
async def api_stats():
    with state_lock:
        wr = (bot_state["winning_trades"]/bot_state["total_trades"]*100) if bot_state["total_trades"] > 0 else 0
        pf = (bot_state["gross_profit"]/bot_state["gross_loss"]) if bot_state["gross_loss"] > 0 else (99 if bot_state["gross_profit"] > 0 else 0)
        
        return {
            "logs": list(bot_state["logs"]),
            "total_trades": bot_state["total_trades"],
            "total_pnl": round(bot_state["total_pnl"], 4),
            "win_rate": round(wr, 1),
            "profit_factor": round(pf, 2),
            "position": bot_state["current_position"],
            "kill_switch_active": bot_state["kill_switch"],
            "indicators": bot_state.get("latest_indicators", {})
        }

class KillSwitchRequest(BaseModel):
    password: str

@app.post("/api/kill-switch")
async def toggle_kill_switch(req: KillSwitchRequest):
    if req.password != KILL_SWITCH_PASSWORD:
        custom_log("⚠️ Cảnh báo: Nhập sai mật khẩu Kill Switch!")
        return {"success": False, "message": "Sai mật khẩu!"}

    with state_lock:
        bot_state["kill_switch"] = not bot_state["kill_switch"]
        status = bot_state["kill_switch"]
        amt = bot_state["position_amt"]
    
    if status and amt != 0:
        try:
            side = 'SELL' if amt > 0 else 'BUY'
            client.futures_create_order(symbol=SYMBOL, side=side, type='MARKET', quantity=abs(amt), reduceOnly=True)
            custom_log("🚨 KILL SWITCH ĐÃ ĐÓNG VỊ THẾ HIỆN TẠI THÀNH CÔNG!")
            
            with state_lock:
                bot_state["current_position"] = "NONE"
                bot_state["position_amt"] = 0.0
                
        except Exception as e:
            custom_log(f"🚨 KILL SWITCH Lỗi khi cố gắng đóng vị thế: {e}")
            
    custom_log(f"🚨 KILL SWITCH: {'BẬT' if status else 'TẮT'}")
    return {"success": True, "kill_switch_active": status}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=5000, log_level="warning")