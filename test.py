import pandas as pd
import numpy as np
import json
from sqlalchemy import create_engine
from datetime import datetime, timedelta

# 最新交易日判断函数
def get_latest_trade_day():
    now = datetime.now()
    hour = now.hour
    today = now.date()
    weekday = today.weekday()

    if weekday >= 5:  # 周六日
        offset = (weekday - 4) if weekday != 6 else 2
        return pd.to_datetime(today - timedelta(days=offset))
    if weekday == 0 and hour < 18:
        return pd.to_datetime(today - timedelta(days=3))
    if hour < 18:
        return pd.to_datetime(today - timedelta(days=1))
    return pd.to_datetime(today)

# PostgreSQL 数据库配置
engine = create_engine("postgresql+psycopg2://postgres:123456@25.tcp.cpolar.top:11324/stock")
OUTPUT_PATH = "macd_latest_buy_signals.json"

# MACD计算
def calc_macd(df):
    df['EMA12'] = df['close'].ewm(span=12, adjust=False).mean()
    df['EMA26'] = df['close'].ewm(span=26, adjust=False).mean()
    df['DIF'] = df['EMA12'] - df['EMA26']
    df['DEA'] = df['DIF'].ewm(span=9, adjust=False).mean()
    df['MACD'] = 2 * (df['DIF'] - df['DEA'])
    df['signal'] = 0
    df.loc[(df['DIF'] > df['DEA']) & (df['DIF'].shift(1) <= df['DEA'].shift(1)), 'signal'] = 1
    return df

# 从数据库读取数据
def get_stock_data(ts_code):
    sql = f"""
        SELECT ts_code, trade_date, open, high, low, close, pre_close, pct_chg,
               vol, bay, ma120, ma250, name
        FROM all_stocks_days
        WHERE ts_code = '{ts_code}'
        ORDER BY trade_date ASC
    """
    df = pd.read_sql(sql, engine)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    return df

# 提取前后窗口
def extract_window(df, center_index, before=20, after=20):
    start = max(center_index - before, 0)
    end = min(center_index + after + 1, len(df))
    return df.iloc[start:end]

# 主流程
def main():
    column_names = [
        "ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg",
        "vol", "bay", "ma120", "ma250", "name"
    ]
    grid_data = []

    # 从本地文件读取股票列表
    stock_codes = pd.read_csv("stock_list.txt", header=None)[0].tolist()
    today_date = get_latest_trade_day()
    print("自动识别最新交易日为：", today_date.strftime('%Y-%m-%d'))

    for ts_code in stock_codes:
        try:
            df = get_stock_data(ts_code)
            if len(df) < 41:
                continue

            df = calc_macd(df)

            # 只处理包含最新交易日的股票
            today_row = df[df['trade_date'] == today_date]
            if today_row.empty:
                continue

            idx = today_row.index[0]
            if df.at[idx, 'signal'] != 1:
                continue

            window_df = extract_window(df, idx, before=20, after=20)

            records = window_df[column_names].copy()
            records['trade_date'] = records['trade_date'].dt.strftime('%Y-%m-%d')
            data_list = records.values.tolist()

            # 添加预测卖点占位
            last_row = data_list[-1].copy()
            last_row[1] = "预测卖点日期"
            last_row[9] = "预测卖出价"
            data_list.append(last_row)

            grid_data.append(data_list)

        except Exception as e:
            print(f"{ts_code} 处理失败：{e}")
            continue

    output = {
        "column_names": column_names,
        "grid_data": grid_data
    }

    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n✅ 已完成，共 {len(grid_data)} 支股票今日出现金叉，结果写入 {OUTPUT_PATH}")

if __name__ == '__main__':
    main()
