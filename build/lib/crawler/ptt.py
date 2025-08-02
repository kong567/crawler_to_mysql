
# import requests
# from bs4 import BeautifulSoup
# import pandas as pd
# from datetime import datetime
# import time
# from crawler.worker import app
# from crawler.mysqlcreate import upload_data_to_mysql_ptt
# import sys

# @app.task()

# def PTT_news(start_index):
#     start_index = int(start_index)
#     def get_full_date(post_url):
#         # headers = {'User-Agent': 'Mozilla/5.0'}
#         # cookies = {'over18': '1'}
#         headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
#         'Accept-Language': 'zh-TW,zh;q=0.9',
#         'Referer': 'https://www.google.com/',
#         'Connection': 'keep-alive'
#         }
#         cookies = {
#                     'over18': '1'
#         }
#         try:
#             res = requests.get(post_url, headers=headers, cookies=cookies)
#             soup = BeautifulSoup(res.text, "html.parser")
#             meta_tags = soup.find_all("span", class_="article-meta-value")
#             if len(meta_tags) >= 4:
#                 date_str = meta_tags[3].text.strip()  # e.g. 'Wed Jun 3 10:58:01 2020'
#                 date_obj = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
#                 return date_obj.strftime("%Y/%m/%d")
#         except Exception as e:
#             print(f"[get_full_date] Error for {post_url}: {e}")
#         return "Unknown"

#     def crawl_page(url):
#         # headers = {'User-Agent': 'Mozilla/5.0'}
#         # cookies = {'over18': '1'}
#         headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
#         'Accept-Language': 'zh-TW,zh;q=0.9',
#         'Referer': 'https://www.google.com/',
#         'Connection': 'keep-alive'
#         }
#         cookies = {
#                     'over18': '1'
#         }
#         res = requests.get(url, headers=headers, cookies=cookies)
#         soup = BeautifulSoup(res.text, "html.parser")
#         articles = soup.find_all("div", class_="r-ent")

#         data_list = []

#         for a in articles:
#             title_div = a.find("div", class_="title")
#             if title_div and title_div.a:
#                 title = title_div.a.text.strip()
#                 href = title_div.a['href']
#                 post_url = "https://www.ptt.cc" + href
#                 full_date = get_full_date(post_url)
#             else:
#                 title = "沒標題"
#                 full_date = "Unknown"

#             pop_div = a.find("div", class_="nrec")
#             pop = pop_div.span.text.strip() if pop_div and pop_div.span else "None"

#             data_list.append({
#                 "標題": title,
#                 "人氣": pop,
#                 "日期": full_date
#             })

#             time.sleep(0.5)  # small delay to avoid being blocked

#         return data_list

#     # === Set your page range ===
#     def get_latest_index(board):
#         url = "https://www.ptt.cc/bbs/" + board +"/index.html"
#         # headers = {'User-Agent': 'Mozilla/5.0'}
#         # cookies = {'over18': '1'}
#         headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
#         'Accept-Language': 'zh-TW,zh;q=0.9',
#         'Referer': 'https://www.google.com/',
#         'Connection': 'keep-alive'
#         }
#         cookies = {
#                     'over18': '1'
#         }
#         res = requests.get(url, headers=headers, cookies=cookies)
#           # ✅ 印出原始 HTML，確認你看到的是正常的 index 頁，還是 "未滿18歲" 或其他錯誤頁面
#         print("====== HTML 頁面內容開始 ======")
#         print(res.text[:2000])  # 印前 2000 字就好
#         print("====== HTML 頁面內容結束 ======")
#         soup = BeautifulSoup(res.text, 'html.parser')
#         a = soup.find_all("a", {"class": "btn wide"})
#         print(f"✅ 找到 {len(a)} 個 .btn.wide 元素")
        
#         # ⚠️ 把使用 a[1] 移進條件內
#         if len(a) > 1:
#             href = a[1]["href"]
#         elif len(a) == 1:
#             href = a[0]["href"]
#         else:
#             print(f"❌ 無法從 {url} 找到足夠的分頁連結 a: {a}")
#             return None
        
        

#         split_href = href.split("/")
#         if not split_href or "index" not in split_href[-1]:
#             print(f"❌ 無法解析 index: href={href}")
#             return None
        
#         latest_index = split_href[-1]

#         delete_words = ["index", ".", "html"]

#         for delete in delete_words:
#             latest_index = latest_index.replace(delete, "")
#         return int(latest_index) + 1 

#     board = "stock"
   



#     # start_index = 8950
#     end_index = get_latest_index(board)

#     if end_index is None:
#         print("❌ 結束任務：無法取得最新 index，可能是被 PTT 擋住或網路錯誤")
#         return
    
#     # ===========================

#     base_url = "https://www.ptt.cc/bbs/Stock/index{}.html"
#     all_data = []

#     for i in range(int(start_index), end_index + 1):
#         url = base_url.format(i)
#         print(f"Crawling: {url}")
#         try:
#             data = crawl_page(url)
#             all_data.extend(data)
#             print(f"爬到第{i}")
#             time.sleep(2.0)
#         except Exception as e:
#             print(f"⚠️ Error at {url}: {e}")
#             continue

#     df = pd.DataFrame(all_data)
#     df = df.rename(columns={"標題": "Title",
#                             "人氣": "Popularity",
#                             "日期": "Date",
#                             })
  

#     df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
#     df = df[df["Title"].notna()]
#     df = df[df["Title"] != "沒標題"]
#     df = df[df["Date"].notna()]
#     # df['Date'] = df['Date'].apply(lambda x: None if pd.isna(x) else x)
#     upload_data_to_mysql_ptt(df)
#     print("輸入進mysql完成")
#     print("✅ Done. Saved to ptt_stock_realdate.csv")


# if __name__ == "__main__":
#     if len(sys.argv) > 1:
#         start_index = sys.argv[1]
#     else:
#         start_index = "9000"  # 或任何你希望的 fallback 值
#     print(f"✅ 進入 main，ptt: {start_index}", flush=True)
#     PTT_news(start_index)




import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import random
import functools
from crawler.worker import app
from crawler.mysqlcreate import upload_data_to_mysql_ptt
import sys



def PTT_news(start_index):
    start_index = int(start_index)

   
    def get_full_date(post_url):
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Referer': 'https://www.google.com/',
            'Connection': 'keep-alive'
        }
        cookies = {'over18': '1'}

        res = requests.get(post_url, headers=headers, cookies=cookies, timeout=10)
        soup = BeautifulSoup(res.text, "html.parser")
        meta_tags = soup.find_all("span", class_="article-meta-value")
        if len(meta_tags) >= 4:
            date_str = meta_tags[3].text.strip()
            date_obj = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Y")
            return date_obj.strftime("%Y/%m/%d")
        return "Unknown"

  
    def crawl_page(url):
        headers = {
            'User-Agent': 'Mozilla/5.0',
            'Accept-Language': 'zh-TW,zh;q=0.9',
            'Referer': 'https://www.google.com/',
            'Connection': 'keep-alive'
        }
        cookies = {'over18': '1'}

        res = requests.get(url, headers=headers, cookies=cookies, timeout=10)
        if "未滿18歲" in res.text or "over18" in res.text:
            raise ValueError("❌ 被導向未滿18頁面")

        soup = BeautifulSoup(res.text, "html.parser")
        articles = soup.find_all("div", class_="r-ent")
        data_list = []

        for a in articles:
            title_div = a.find("div", class_="title")
            if title_div and title_div.a:
                title = title_div.a.text.strip()
                href = title_div.a['href']
                post_url = "https://www.ptt.cc" + href
                full_date = get_full_date(post_url)
            else:
                title = "沒標題"
                full_date = "Unknown"

            pop_div = a.find("div", class_="nrec")
            pop = pop_div.span.text.strip() if pop_div and pop_div.span else "None"

            data_list.append({"標題": title, "人氣": pop, "日期": full_date})
            time.sleep(0.5 + random.uniform(0.2, 0.8))  # 模擬人類行為

        return data_list

   
    base_url = "https://www.ptt.cc/bbs/Stock/index{}.html"
    all_data = []

    
    url = base_url.format(start_index)
    print(f"Crawling: {url}")
    try:
        data = crawl_page(url)
        all_data.extend(data)
        print(f"✅ 爬到第 {start_index} 頁，共 {len(data)} 筆")
        time.sleep(3 + random.uniform(0, 1.5))
    except Exception as e:
        print(f"⚠️ Error at {url}: {e}")
       

    df = pd.DataFrame(all_data).rename(columns={"標題": "Title", "人氣": "Popularity", "日期": "Date"})
    df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
    df = df[df["Title"].notna() & (df["Title"] != "沒標題") & df["Date"].notna()]

    upload_data_to_mysql_ptt(df)
    print("✅ 輸入 MySQL 完成")

if __name__ == "__main__":
    start_index = sys.argv[1] 
    print(f"✅ 進入 main，ptt: {start_index}", flush=True)
    PTT_news(start_index)