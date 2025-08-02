import requests
import pandas as pd
import time
import random
from datetime import datetime, timedelta
from crawler.mysqlcreate import upload_data_to_mysql_cnyes_headlines
import sys
import logging


# 取今天的日期

def crawler_cnyes_headlines_daily(today) :
    start_ts = int(datetime(today.year, today.month, today.day, 0, 0, 0).timestamp())
    end_ts = int(datetime(today.year, today.month, today.day, 23, 59, 59).timestamp())


    # headers 設定
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://news.cnyes.com/news/cat/headline"
    }

    # 初始化
    results = []
    page = 1
    while True:
        url = (
            f"https://api.cnyes.com/media/api/v1/newslist/category/headline"
            f"?startAt={start_ts}&endAt={end_ts}&limit=100&page={page}"
        )

        time.sleep(5)
        res = requests.get(url, headers=headers)
        res.encoding = "utf-8"

        try:
            data = res.json()
            items = data["items"]["data"]
        except Exception as e:
            logging.error(f"發生錯誤: {e}", exc_info=True)
            break

        if not items:
            break

        for item in items:
            timestamp = item["publishAt"]
            pub_time = datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M")
            title = item["title"]
            news_id = item["newsId"]
            link = f"https://news.cnyes.com/news/id/{news_id}"
            results.append([pub_time, title, link])

        page += 1
    df = pd.DataFrame(results, columns=["pub_time", "Title", "link"])
    upload_data_to_mysql_cnyes_headlines(df)
    filename = "mysql"
    print(f"已儲存 {len(df)} 筆新聞到 {filename}")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ 請輸入日期參數，例如：python script.py 2024-12-01")
        sys.exit(1)
    try:
        news_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()   

    except ValueError:
        print("❌ 日期格式錯誤，請使用 YYYY-MM-DD，例如：2024-12-01")
        sys.exit(1)

    print(f"✅ 進入 main，cnyes_headlines: {news_date}", flush=True)

    crawler_cnyes_headlines_daily(news_date)

# today = datetime.today()