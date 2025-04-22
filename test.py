import pandas as pd
import psycopg2
import json
from datetime import datetime, timedelta

# 数据库配置
db_config = {
    "host": "25.tcp.cpolar.top",
    "port": 11324,
    "database": "stock",
    "user": "postgres",
    "password": "123456"
}

def calculate_macd(df):
    df = df.sort_values('trade_date')
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['dif'] = df['ema12'] - df['ema26']
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    df['macd'] = 2 * (df['dif'] - df['dea'])
    df['buy'] = ((df['dif'] > df['dea']) & (df['dif'].shift(1) <= df['dea'].shift(1))).astype(int)
    return df

def fetch_stock_codes(conn):
    sql = "SELECT DISTINCT ts_code FROM all_stocks_days"
    return pd.read_sql(sql, conn)['ts_code'].tolist()

def fetch_stock_data(conn, ts_code):
    sql = """
        SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg,
               vol, bay, ma120, ma250, name
        FROM all_stocks_days
        WHERE ts_code = %s
        ORDER BY trade_date
    """
    return pd.read_sql(sql, conn, params=(ts_code,))

def apply_stop_logic(df, profit_thresh=0.05, loss_thresh=-0.03):
    df['sell'] = 0.0
    buy_indices = df.index[df['buy'] == 1].tolist()
    for buy_idx in buy_indices:
        buy_price = df.at[buy_idx, 'close']
        for future_idx in range(buy_idx + 1, len(df)):
            current_price = df.at[future_idx, 'close']
            rate = (current_price - buy_price) / buy_price
            if rate >= profit_thresh or rate <= loss_thresh:
                df.at[future_idx, 'sell'] = current_price
                break
    return df

def get_latest_trade_day():
    now = datetime.now()
    today = now.date()
    hour = now.hour
    weekday = today.weekday()
    if weekday == 5: return today - timedelta(days=1)
    if weekday == 6: return today - timedelta(days=2)
    if weekday == 0 and hour < 18: return today - timedelta(days=3)
    if hour < 18: return today - timedelta(days=1)
    return today

def process_stock(conn, ts_code):
    df = fetch_stock_data(conn, ts_code)
    if df.empty or len(df) < 35:
        return None, None
    df = calculate_macd(df)
    df = apply_stop_logic(df)
    df['bay'] = df['pre_close'].where(df['buy'] == 1, 0)

    # 判断是否在最新交易日有买点
    latest_trade_day_str = get_latest_trade_day().strftime('%Y%m%d')
    latest_row = df[df['trade_date'] == latest_trade_day_str]
    if latest_row.empty or latest_row['buy'].iloc[0] ==0:
        return None, None

    # 提取最近20天的数据
    latest_data = df.tail(20).copy()
    data = latest_data[[
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg",
        "vol", "bay", "ma120", "ma250", "name", "sell"
    ]].values.tolist()

    # 平均收益率
    profits = []
    for idx in df.index[df['buy'] == 1]:
        buy_price = df.at[idx, 'close']
        for j in range(idx + 1, len(df)):
            sell_price = df.at[j, 'sell']
            if sell_price != 0:
                profits.append((sell_price - buy_price) / buy_price)
                break
    avg_profit = round(sum(profits) / len(profits), 4) if profits else 0.0

    return [data], {"ts_code": ts_code, "avg_profit": avg_profit}

def main():
    print("🚀 正在连接数据库...")
    conn = psycopg2.connect(**db_config)
    print("✅ 数据库连接成功")

    column_names = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg",
        "vol", "bay", "ma120", "ma250", "name", "sell"
    ]
    strategy_scores = []
    stock_count = 0
    has_written = False  # 👈 用于控制逗号输出

    with open("macd_result.json", "w", encoding='utf-8') as f:
        f.write('{\n')
        f.write('"column_names": ' + json.dumps(column_names, ensure_ascii=False) + ',\n')
        f.write('"data": [\n')

        ts_codes = fetch_stock_codes(conn)
        total = len(ts_codes)
        print(f"📈 共找到 {total} 支股票，开始处理")

        for idx, ts_code in enumerate(ts_codes):
            print(f"[{idx+1}/{total}] 正在处理：{ts_code}", end="")
            stock_data, score_info = process_stock(conn, ts_code)
            if stock_data:
                if has_written:
                    f.write(',\n')
                json.dump(stock_data[0], f, ensure_ascii=False)
                stock_count += 1
                has_written = True
                print(" ✅ 写入成功")
                if score_info:
                    print(f"  ↳ 平均收益率: {score_info['avg_profit']*100:.2f}%")
            else:
                print(" ❌ 未写入（无买点）")
            if score_info:
                strategy_scores.append(score_info)

        f.write('\n],\n')
        f.write(f'"stock_count": {stock_count}\n')
        f.write('}')

    conn.close()

    print("\n📊 策略表现前 10（按平均收益率）:")
    for rank, item in enumerate(sorted(strategy_scores, key=lambda x: x['avg_profit'], reverse=True)[:10], start=1):
        print(f"{rank}. {item['ts_code']} - 平均收益率: {item['avg_profit']*100:.2f}%")

if __name__ == "__main__":
    main()
