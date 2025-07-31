
import requests
import bs4 as bs
import pandas as pd
from datetime import datetime, timedelta
import tabulate
import jieba.analyse
import jieba
import logging
from crawler.worker import app
from crawler.mysqlcreate import upload_data_to_mysql_MagaBank_NEWS
import sys
import time





def Bank_NEWS_daily(news_date):
    table = []
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
    }

    # 只抓今天
    today = news_date
    date_str = f"{today.year}-{today.month}-{today.day}"
    formatted_date = today.strftime("%Y/%m/%d")

    for page in range(1, 6):  # 每日最多5頁
        url = f"https://fund.megabank.com.tw/w/wp/wu01megaNews.djhtm?A=NA&B={date_str}&C=NA&Page={page}"
        response = requests.get(url, headers=headers)
        html = bs.BeautifulSoup(response.text, features="html.parser")

        for tr in html.find_all('tr'):
            en_date = tr.find('td', class_=['wfb2c', 'wfb5c'])
            en_title = tr.find('a')

            if en_date and en_title and en_date.text.strip() == formatted_date:
                title_final = en_title.text.strip()
                url_final = "https://fund.megabank.com.tw" + en_title['href']
                tags = jieba.analyse.extract_tags(title_final)

                data = {
                    "日期": en_date.text.strip(),
                    "標題": title_final,
                    "連結": url_final,
                    "標籤": tags
                }

                if data not in table:
                    table.append(data)
    print(f"--------------------------------------{today}--------------------------------------")
    df = pd.DataFrame(table)

    df["標籤"] = df["標籤"].apply(lambda x: ", ".join(x))

    df = df.rename(columns={"日期": "Date",
                        "標題": "Title",
                        "連結": "Link",
                        "標籤": "Label"})
    print("爬蟲完成")

    upload_data_to_mysql_MagaBank_NEWS(df)
    print("輸入進mysql完成")
    time.sleep(4)        



if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ 請輸入日期參數，例如：python script.py 2024-12-01")
        sys.exit(1)
    try:
        news_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()   

    except ValueError:
        print("❌ 日期格式錯誤，請使用 YYYY-MM-DD，例如：2024-12-01")
        sys.exit(1)

    print(f"✅ 進入 main，Bank_NEWS: {news_date}", flush=True)

    Bank_NEWS_daily(news_date)

    
    
   
# news_date = datetime.today().date()




