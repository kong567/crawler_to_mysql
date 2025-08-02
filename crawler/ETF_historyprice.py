# === 1 ：匯入套件 ===


# import yfinance as yf
import pandas as pd
import time
from loguru import logger
from datetime import datetime
# from crawler.mysqlcreate import upload_data_to_mysql_ETF_historyprice
import logging
import sys
import requests



import requests
import pandas as pd

logging.basicConfig(
    stream=sys.stdout,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
def historyprice(ticker) :
    end_date = pd.Timestamp.today().strftime('%Y-%m-%d')
    url = "https://api.finmindtrade.com/api/v4/data"
    token = "" # 參考登入，獲取金鑰
    headers = {"Authorization": f"Bearer {token}"}
    parameter = {
        "dataset": "TaiwanStockPrice",
        "data_id": ticker,
        "start_date": "2020-04-02",
        "end_date": str(end_date),
    }
    resp = requests.get(url, headers=headers, params=parameter)
    data = resp.json()
    data = pd.DataFrame(data["data"])
    print(data.head())
    
    data = data.rename(columns={"close": "Close", "volume": "Volume"})
    data["Adj_Close"] = data["Close"]  # 補上缺失欄位
    data["Stock_id"] = ticker
    data = data[["date", "Stock_id", "Close", "Adj_Close", "Volume"]]
    data = data.rename(columns={"date": "Date"})
    print(data.head())
    logger.info(data.tail())
    print(f"✅ 寫入前資料筆數：{len(data)}")
    logging.info("已完成爬蟲，準備寫入資料庫")
    time.sleep(10)
    # upload_data_to_mysql_ETF_historyprice(data)
    # logging.info("已寫入 MySQL")




if __name__ == "__main__":
    ticker = sys.argv[1]
    print(f"✅ 進入 main，historyprice: {ticker}", flush=True)
    historyprice(ticker)

# def historyprice(ticker) :
#     print(f"ETF_historyprice.py 啟動，收到的 ticker 是：{sys.argv[1]}")

#     headers = {
#     "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
# }
#     session = requests.Session()
#     session.headers.update(headers)
#     # === 2 ：設定參數 ===
#     start_date = '2020-01-01'
#     end_date = pd.Timestamp.today().strftime('%Y-%m-%d')

#     # === 3 ：下載資料（保留原始 Close 與 Adj Close）===
#     time.sleep(20)  # 避免過於頻繁請求
#     df = yf.download(ticker, start=start_date, end=end_date, group_by='ticker', auto_adjust=False, session=session))

#     # 提取該股票對應的資料欄位（扁平化）
#     # df_single = df.xs(ticker, level='Ticker', axis=1)
    
  
#     df_single = df.xs(ticker, level='Ticker', axis=1)
    
    
#     # 判斷是否包含 Adj Close 欄位
#     has_adj = 'Adj Close' in df_single.columns

#     # 顯示前 5 筆資料
#     print("前五筆資料：")
#     print(df_single.head())


#     # 動態選擇欄位
#     columns_to_save = ['Close', 'Volume']
#     if has_adj:
#         columns_to_save.insert(1, 'Adj Close')  # 加在 Close 後面

#     df = df_single[columns_to_save].rename(columns={"Adj Close": "Adj_Close"})
#     df = df.reset_index()
#     df['Stock_id'] = ticker
#     logger.info(df.tail(1))
#     print(f"✅ 寫入前資料筆數：{len(df)}")
#     logging.info("已完成爬蟲，準備寫入資料庫")
#     upload_data_to_mysql_ETF_historyprice(df)
#     logging.info("已寫入 MySQL")





    