import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime, date
import time
import random
import functools
from crawler.worker import app
from crawler.mysqlcreate import upload_data_to_mysql_ptt
import sys


BASE_URL = "https://www.ptt.cc"
BOARD_URL = f"{BASE_URL}/bbs/Stock/index.html"
HEADERS = {"User-Agent": "Mozilla/5.0"}
COOKIES = {"over18": "1"}

def get_page_soup(url):
    res = requests.get(url, headers=HEADERS, cookies=COOKIES)
    return BeautifulSoup(res.text, "html.parser")

def get_last_page_number():
    soup = get_page_soup(BOARD_URL)
    btns = soup.find_all("a", class_="btn wide")
    for btn in btns:
        if "上頁" in btn.text:
            href = btn["href"]
            page_num = int(href.split("index")[1].split(".html")[0])
            return page_num + 1
    return None

def get_post_date(post_url):
    try:
        soup = get_page_soup(post_url)
        meta_tags = soup.find_all("span", class_="article-meta-value")
        if len(meta_tags) >= 4:
            date_str = meta_tags[3].text.strip()
            date_obj = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
            return date_obj.date()
    except Exception as e:
        print(f"Error parsing date from {post_url}: {e}")
    return None

def crawl_today_posts(target_today: date):
   
    print(f"Today's date: {target_today}")
    latest_page = get_last_page_number()
    print(f"Latest page number: {latest_page}")
    all_posts = []
    page = latest_page
    no_today_count = 0

    while page > 0:
        url = f"{BASE_URL}/bbs/Stock/index{page}.html"
        print(f"📄 Crawling {url}")
        soup = get_page_soup(url)
        articles = soup.find_all("div", class_="r-ent")

        today_posts_this_page = 0

        for a in articles:
            title_div = a.find("div", class_="title")
            if not title_div or not title_div.a:
                continue

            title = title_div.a.text.strip()
            href = BASE_URL + title_div.a["href"]
            post_date = get_post_date(href)

            if post_date == target_today:
                pop_div = a.find("div", class_="nrec")
                pop = pop_div.span.text.strip() if pop_div and pop_div.span else "None"
                all_posts.append({"標題": title, "人氣": pop, "日期": post_date})
                today_posts_this_page += 1
                print(f"✅ 第{today_posts_this_page}輸入 MySQL 完成")

            time.sleep(0.5)

        if today_posts_this_page == 0:
            no_today_count += 1
            if no_today_count >= 2:
                print("No posts from today found in two consecutive pages. Stopping crawl.")
                break
        else:
            no_today_count = 0

        page -= 1
        
        
        time.sleep(1)
    df = pd.DataFrame(all_posts).rename(columns={"標題": "Title", "人氣": "Popularity", "日期": "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df = df[df["Title"].notna() & (df["Title"] != "沒標題") & df["Date"].notna()]
    upload_data_to_mysql_ptt(df)
    print("✅ 輸入 MySQL 完成")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ 請輸入日期參數，例如：python script.py 2024-12-01")
        sys.exit(1)

    try:
        target_today = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()

    except ValueError:
        print("❌ 日期格式錯誤，請使用 YYYY-MM-DD，例如：2024-12-01")
        sys.exit(1)

    print(f"✅ 進入 main，ptt: {target_today}", flush=True)
    crawl_today_posts(target_today)


# # 執行主程式
# if __name__ == "__main__":
#     posts = crawl_today_posts()

#     if posts:
#         df = pd.DataFrame(posts)
#         today_str = datetime.now().strftime("%Y%m%d")

#         print(f"✅ 共取得 {len(posts)} 筆今日文章")
#     else:
#         print("⚠️ 今日未找到任何文章")