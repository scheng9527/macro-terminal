import os
import json
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime, timezone, timedelta

# 强制设置时区为 UTC+8 (亚洲时间)
tz = timezone(timedelta(hours=8))
current_time = datetime.now(tz)
print(f"[{current_time.strftime('%Y-%m-%d %H:%M:%S')}] 🚀 云端宏观数据引擎启动...")

HEADERS = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36'}

def fetch_cboe_and_yield():
    print("-> 获取 市场核心数据 (VIX/SKEW/美债利差)...")
    try:
        vix = float(yf.download("^VIX", period="1d", progress=False, auto_adjust=False)['Close'].iloc[-1].item())
        skew = float(yf.download("^SKEW", period="1d", progress=False, auto_adjust=False)['Close'].iloc[-1].item())
        us10y = float(yf.download("^TNX", period="1d", progress=False, auto_adjust=False)['Close'].iloc[-1].item())
        us2y = float(yf.download("^IRX", period="1d", progress=False, auto_adjust=False)['Close'].iloc[-1].item()) 
        # 注: IRX 是 13周国债，仅作演示。实际 2Y 数据源建议用 FRED API 替换。这里先写死一个演示利差。
        
        return {"vix": vix, "skew": skew, "yield_spread": -38.5}, True
    except Exception as e: 
        print(f"   [抓取失败] 雅虎财经: {e}")
        return {"vix": 0.0, "skew": 0.0, "yield_spread": 0.0}, False

def fetch_squeezemetrics():
    print("-> 获取 SqueezeMetrics (DIX/GEX)...")
    try:
        df = pd.read_csv("https://squeezemetrics.com/monitor/static/DIX.csv", storage_options=HEADERS)
        if df.empty: return {"dix": 0.0, "gex": 0.0}, False
        return {"dix": float(df.iloc[-1]['dix']), "gex": float(df.iloc[-1]['gex'])}, True
    except Exception as e: 
        print(f"   [抓取失败] SqueezeMetrics: {e}")
        return {"dix": 0.0, "gex": 0.0}, False

def fetch_polymarket():
    print("-> 获取 Polymarket 宏观大事件...")
    try:
        url = "https://gamma-api.polymarket.com/events?limit=3&active=true&closed=false&order=volume24hr"
        res = requests.get(url, headers=HEADERS, timeout=10).json()
        events = []
        for e in res[:3]:
            try: outcome = float(e['markets'][0]['outcomePrices'][0]) * 100
            except: outcome = 0.0
            events.append({"title": e.get('title', '未知事件'), "prob": outcome})
        return events, True
    except Exception as e: 
        print(f"   [抓取失败] Polymarket: {e}")
        return [], False

def run_pipeline():
    today_date = current_time.strftime("%Y-%m-%d")
    
    # 1. 执行抓取
    cboe_data, cboe_ok = fetch_cboe_and_yield()
    sq_data, sq_ok = fetch_squeezemetrics()
    poly_data, poly_ok = fetch_polymarket()
    
    # 2. 熔断机制
    if not cboe_ok:
        print("❌ 核心数据(VIX)抓取失败，触发熔断，停止归档防污染。")
        return

    # 3. 组装今日数据
    final_data = {
        "timestamp": current_time.strftime("%Y-%m-%d %H:%M:%S CST"),
        "date": today_date,
        "metrics": {**cboe_data, **sq_data},
        "polymarket": poly_data
    }
    
    # 4. 追加到主时间序列文件 (master_series.json)
    os.makedirs("database", exist_ok=True)
    master_file = "database/master_series.json"
    
    if os.path.exists(master_file):
        with open(master_file, "r", encoding="utf-8") as f:
            try: history_data = json.load(f)
            except: history_data = []
    else:
        history_data = []
        
    # 防重与追加：如果今天已经抓过了，就覆盖最后一条；否则追加新的一条
    if len(history_data) > 0 and history_data[-1]["date"] == today_date:
        history_data[-1] = final_data
    else:
        history_data.append(final_data)
        
    # 容量控制：永远只保留过去 365 天的数据，保证前端加载极速
    history_data = history_data[-365:]
    
    with open(master_file, "w", encoding="utf-8") as f:
        # 使用 separators 压缩 JSON 体积
        json.dump(history_data, f, ensure_ascii=False, separators=(',', ':'))
        
    print(f"✅ 数据成功归档！master_series.json 当前总天数: {len(history_data)} 天")

if __name__ == "__main__":
    run_pipeline()
