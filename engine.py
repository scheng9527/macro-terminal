import os
import json
import requests
import pandas as pd
import yfinance as yf
from datetime import datetime

print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 🚀 宏观数据抓取引擎启动...")

# 全局请求伪装头
HEADERS = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}

# ==========================================
# 模块一：数据抓取 (带严格容错)
# ==========================================
def fetch_squeezemetrics():
    print("-> 获取 SqueezeMetrics (DIX/GEX)...")
    try:
        df = pd.read_csv("https://squeezemetrics.com/monitor/static/DIX.csv", storage_options=HEADERS)
        if df.empty: raise ValueError("获取到了空数据")
        return {"dix": float(df.iloc[-1]['dix']), "gex": float(df.iloc[-1]['gex'])}, True
    except Exception as e: 
        print(f"   [抓取失败] SqueezeMetrics: {e}")
        return {"dix": 0.0, "gex": 0.0}, False

def fetch_cboe_volatility():
    print("-> 获取 CBOE (VIX/SKEW) 数据...")
    try:
        # yf.download 失败时会返回空 DataFrame，必须加上 empty 校验
        vix_df = yf.download("^VIX", period="1d", progress=False)
        skew_df = yf.download("^SKEW", period="1d", progress=False)
        
        if vix_df.empty or skew_df.empty:
            raise ValueError("Yahoo Finance 返回了空表格，可能网络被拦截")
            
        vix = float(vix_df['Close'].iloc[-1].item())
        skew = float(skew_df['Close'].iloc[-1].item())
        return {"vix": vix, "skew": skew}, True
    except Exception as e: 
        print(f"   [抓取失败] CBOE: {e}")
        return {"vix": 0.0, "skew": 0.0}, False

def fetch_polymarket_macro():
    print("-> 获取 Polymarket 宏观大事件...")
    try:
        url = "https://gamma-api.polymarket.com/events?limit=5&active=true&closed=false&order=volume24hr"
        res = requests.get(url, headers=HEADERS, timeout=10).json()
        events = []
        for e in res[:5]:
            try: outcome = e['markets'][0]['outcomePrices'][0]
            except: outcome = "0"
            events.append({"title": e.get('title', '未知事件'), "top_outcome": outcome})
        
        if not events: raise ValueError("未能提取到任何事件")
        return events, True
    except Exception as e: 
        print(f"   [抓取失败] Polymarket: {e}")
        return [{"title": "获取数据失败", "top_outcome": "0"}], False

# ==========================================
# 模块二：AI 预留接口 (Placeholder)
# ==========================================
def ai_module_placeholder(market_data):
    return None 

# ==========================================
# 模块三：执行、校验与熔断归档
# ==========================================
def run_pipeline():
    today_date = datetime.now().strftime("%Y-%m-%d")
    
    # 1. 执行抓取并获取状态
    sq_data, sq_ok = fetch_squeezemetrics()
    cboe_data, cboe_ok = fetch_cboe_volatility()
    poly_data, poly_ok = fetch_polymarket_macro()
    
    # 2. 组装数据
    market_data = {
        "metrics": {**sq_data, **cboe_data},
        "polymarket": poly_data
    }
    
    # 3. 核心：熔断校验机制 (Fail-Fast)
    # 如果最核心的 CBOE(波动率) 和 Polymarket(大事) 都失败了，说明网络彻底断了，拒绝归档！
    if not cboe_ok and not poly_ok:
        print("\n❌ 致命错误：核心数据源全部抓取失败！")
        print("❌ 熔断机制触发：本次运行取消，拒绝写入数据库以防止数据污染。")
        print("💡 建议排查：请检查你的 Mac 终端网络代理设置。")
        return # 直接终止，不往后走了
    
    # 4. 正常流转：调用 AI (预留)
    ai_analysis = ai_module_placeholder(market_data)
    final_output = {
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "date": today_date,
        "raw_data": market_data,
        "ai_analysis": ai_analysis
    }
    
    # 5. 安全归档
    base_dir = "database"
    history_dir = os.path.join(base_dir, "history")
    os.makedirs(history_dir, exist_ok=True)
    
    history_file = os.path.join(history_dir, f"{today_date}.json")
    with open(history_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)
        
    latest_file = os.path.join(base_dir, "latest.json")
    with open(latest_file, "w", encoding="utf-8") as f:
        json.dump(final_output, f, ensure_ascii=False, indent=2)
        
    print(f"\n✅ 任务完成！数据已安全校验并归档至 database/history/{today_date}.json")

if __name__ == "__main__":
    run_pipeline()