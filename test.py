# 导入模块
import pandas as pd
import psycopg2
import json
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import math

def clean_nan_for_json(data):
    """递归将数据中的 NaN 替换为 None，确保 JSON 中是 null"""
    if isinstance(data, float) and math.isnan(data):
        return None
    elif isinstance(data, list):
        return [clean_nan_for_json(i) for i in data]
    elif isinstance(data, dict):
        return {k: clean_nan_for_json(v) for k, v in data.items()}
    return data


# 数据库配置
db_config = {
    "host": "25.tcp.cpolar.top",
    "port": 11324,
    "database": "stock",
    "user": "postgres",
    "password": "123456"
}

def calculate_macd(df):
    # 按日期升序
    df = df.sort_values('trade_date')
    # 计算EMA
    df['ema12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['ema26'] = df['close'].ewm(span=26, adjust=False).mean()
    # 计算快线DIF
    df['dif'] = df['ema12'] - df['ema26']
    # 计算慢线DEA
    df['dea'] = df['dif'].ewm(span=9, adjust=False).mean()
    # 计算MACD柱状图
    df['macd'] = 2 * (df['dif'] - df['dea'])
    # 快线穿过慢线
    # 若今天 DIF > DEA 且 昨天 DIF <= DEA → 金叉出现，标记为 1（买点）
    df['buy'] = ((df['dif'] > df['dea']) & (df['dif'].shift(1) <= df['dea'].shift(1))).astype(int)
    return df

# 查询所有股票代码
def fetch_stock_codes(engine):
    sql = "SELECT DISTINCT ts_code FROM all_stocks_days"
    return pd.read_sql(sql, engine)['ts_code'].tolist()

# 查询当前计算股票的完整日k数据
def fetch_stock_data(engine, ts_code):
    sql = """
        SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg,
               vol, bay, ma120, ma250, name
        FROM all_stocks_days
        WHERE ts_code = %s
        ORDER BY trade_date
    """
    return pd.read_sql(sql, engine, params=(ts_code,))

# 止盈止损策略
def apply_stop_logic(df, profit_thresh=0.05, loss_thresh=-0.03):
    # 添加卖点 0未卖出
    df['sell'] = 0.0
    # 找出所有卖点的索引
    buy_indices = df.index[df['buy'] == 1].tolist()
    # 从买点向后查找，是否达到 5% 盈利或 3% 止损阈值，若达到则记录 sell 值
    for buy_idx in buy_indices:
        buy_price = df.at[buy_idx, 'close']
        for future_idx in range(buy_idx + 1, len(df)):
            current_price = df.at[future_idx, 'close']
            rate = (current_price - buy_price) / buy_price
            if rate >= profit_thresh or rate <= loss_thresh:
                df.at[future_idx, 'sell'] = current_price
                break
    return df

# 计算最新交易日
def get_latest_trade_day():
    now = datetime.now()
    today = now.date()
    hour = now.hour
    weekday = today.weekday()

    if weekday == 5:  # 周六
        latest_day = today - timedelta(days=1)
    elif weekday == 6:  # 周日
        latest_day = today - timedelta(days=2)
    elif weekday == 0 and hour < 18:  # 周一18点前
        latest_day = today - timedelta(days=3)
    elif hour < 18:  # 工作日18点前
        latest_day = today - timedelta(days=1)
    else:  # 工作日18点后
        latest_day = today

    return latest_day.strftime("%Y-%m-%d")  # 返回格式化字符串

# 处理单只股票
def process_stock(engine, ts_code):
    df = fetch_stock_data(engine, ts_code)
    # 若数据为空或不足 35 条，不处理
    if df.empty or len(df) < 35:
        return None, None
    #计算指标、止盈止损、设置 bay（前一天收盘价）
    df = calculate_macd(df)
    df = apply_stop_logic(df)
    df['bay'] = df['pre_close'].where(df['buy'] == 1, 0)

    # 判断是否在最新交易日有买点
    latest_trade_day_str = get_latest_trade_day()
    # latest_trade_day_str = "2025-04-18"
    # print(latest_trade_day_str)
    # print(df['trade_date'])
    latest_row = df[df['trade_date'] == latest_trade_day_str]
    if latest_row.empty or latest_row['buy'].iloc[0] == 0:
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

# def print_buy_signals(engine, ts_code="000001.SZ"):
#     df = fetch_stock_data(engine, ts_code)
#     df = calculate_macd(df)

#     # 找出所有 buy=1 的行，也就是金叉点
#     buy_signals = df[df['buy'] == 1]

#     if buy_signals.empty:
#         print(f"❌ 股票 {ts_code} 没有出现过买点（金叉）")
#     else:
#         print(f"\n📌 股票 {ts_code} 的所有买点（MACD金叉）：")
#         for _, row in buy_signals.iterrows():
#             print(f"👉 日期：{row['trade_date']}，收盘价：{row['close']:.2f}")

def main():
    print("正在连接数据库...")
    engine = create_engine("postgresql+psycopg2://postgres:123456@25.tcp.cpolar.top:11324/stock")

    print("数据库连接成功")
    # print_buy_signals(conn, "000001.SZ")

    column_names = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg",
        "vol", "bay", "ma120", "ma250", "name", "sell"
    ]
    strategy_scores = []
    stock_count = 0
    has_written = False  # 用于控制逗号输出

    with open("macd_result.json", "w", encoding='utf-8') as f:
        f.write('{\n')
        f.write('"column_names": ' + json.dumps(column_names, ensure_ascii=False) + ',\n')
        f.write('"data": [\n')

        ts_codes = fetch_stock_codes(engine)
        total = len(ts_codes)
        print(f"📈 共找到 {total} 支股票，开始处理")

        for idx, ts_code in enumerate(ts_codes):
            print(f"[{idx+1}/{total}] 正在处理：{ts_code}", end="")
            stock_data, score_info = process_stock(engine, ts_code)
            if stock_data:
                if has_written:
                    f.write(',\n')
                json.dump(clean_nan_for_json(stock_data[0]), f, ensure_ascii=False)
                stock_count += 1
                has_written = True
                print(" 写入成功")
                if score_info:
                    print(f" 平均收益率: {score_info['avg_profit']*100:.2f}%")
            else:
                print(" 未写入（无买点）")
            if score_info:
                strategy_scores.append(score_info)

        f.write('\n],\n')
        f.write(f'"stock_count": {stock_count}\n')
        f.write('}')

    # engine.close()

    print("\n策略表现前 10（按平均收益率）:")
    for rank, item in enumerate(sorted(strategy_scores, key=lambda x: x['avg_profit'], reverse=True)[:10], start=1):
        print(f"{rank}. {item['ts_code']} - 平均收益率: {item['avg_profit']*100:.2f}%")

if __name__ == "__main__":
    main()
