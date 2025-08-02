# install yfinance

import yfinance as yf
import pandas as pd
from datetime import datetime
from crawler.worker import app
from crawler.mysqlcreate import upload_data_to_mysql_vix
import sys
from loguru import logger



import requests
from bs4 import BeautifulSoup  # 用來解析 HTML



def vix_data(Volatility_Index):
    # # ticker for the VIX
    # vix_ticker = yf.Ticker(Volatility_Index)

    # # Get today's date
    # today = datetime.today().strftime('%Y-%m-%d')

    # # Use the history method to fetch the data(DF)
    # vix_data = vix_ticker.history(start="2020-01-01", end=today)

    # # Resetting the index to have Date as a column
    # vix_data = vix_data.reset_index()

    # vix_data['Date'] = vix_data['Date'].dt.date

    # # Round the Close prices to 2 decimal places
    # vix_data['Close'] = vix_data['Close'].round(2)
    url = "https://www.stockq.org/index/VIX.php"

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    }

    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        print("取得資料成功！")
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 範例：找出所有表格資料
        table = soup.find("tr", {"class" : "row1"})
        # print (table)
        date = table.find_all("td")[0].text
        close = table.find_all("td")[1].text
        print (date, close)
    else:
        print(f"取得資料失敗，狀態碼： {response.status_code}")
    vix_data_list = [
                {
                    "Date": date,
                    "Close": close
                        }
            ]
# Selecting columns
    vix_data = pd.DataFrame(vix_data_list)
    print(vix_data)
    print("爬蟲完成")
    upload_data_to_mysql_vix(vix_data)
    print("輸入進mysql完成")

if __name__ == "__main__":
    Volatility_Index = sys.argv[1]
    print(f"✅ 進入 main，vix_data: {Volatility_Index}", flush=True)
    vix_data(Volatility_Index)