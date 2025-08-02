import pandas as pd
import os
from sqlalchemy import create_engine  # 建立資料庫連線的工具（SQLAlchemy）


from crawler.config import MYSQL_ACCOUNT, MYSQL_HOST, MYSQL_PASSWORD, MYSQL_PORT

address = f"mysql+pymysql://{MYSQL_ACCOUNT}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/crawlerDB"



def sentiment_analysis(address):
    engine = create_engine(address)
    # --- 設定路徑 ---
    base_path = os.path.dirname(__file__)  # 自動抓取此腳本所在的資料夾路徑

    # --- 鉅亨與兆豐詞庫 ---
    positive_words = pd.read_csv(os.path.join(base_path, "positive.txt"), header=None)[0].dropna().tolist()
    negative_words = pd.read_csv(os.path.join(base_path, "negative.txt"), header=None)[0].dropna().tolist()




    def get_sentiment_score(title, pos_words, neg_words):
        if pd.isna(title):
            return 0
        pos = sum(word in title for word in pos_words)
        neg = sum(word in title for word in neg_words)
        return pos - neg

    def left_side_label(score):
        if score > 0:
            return -1
        elif score < 0:
            return 1
        else:
            return 0

    # --- 鉅亨與兆豐 ---

    query = "SELECT * FROM 	cnyes_headlines"
    df_cnyes = pd.read_sql(query, engine)

    df_cnyes.columns = df_cnyes.columns.str.strip()
    df_cnyes = df_cnyes.rename(columns={"pub_time": "日期", "Title": "title"})
    df_cnyes["來源"] = "鉅亨"
    df_cnyes["日期"] = pd.to_datetime(df_cnyes["日期"]).dt.date

    query = "SELECT * FROM 	MagaBank_NEWS"
    df_mega = pd.read_sql(query, engine)

    df_mega = df_mega.rename(columns={"Title": "title"})
    df_mega["來源"] = "兆豐"
    df_mega["日期"] = pd.to_datetime(df_mega["Date"]).dt.date

    df_all = pd.concat([df_cnyes[["日期", "title", "來源"]], df_mega[["日期", "title", "來源"]]], ignore_index=True)
    df_all["每日原始總分"] = df_all["title"].apply(lambda x: get_sentiment_score(x, positive_words, negative_words))
    df_all_grouped = df_all.groupby(["日期", "來源"])["每日原始總分"].sum().reset_index()
    df_all_grouped["左側情緒分類"] = df_all_grouped["每日原始總分"].apply(left_side_label)

    df_cnyes_final = df_all_grouped[df_all_grouped["來源"] == "鉅亨"].rename(columns={
        "每日原始總分": "鉅亨_每日原始總分",
        "左側情緒分類": "鉅亨_左側情緒分類"
    })[["日期", "鉅亨_每日原始總分", "鉅亨_左側情緒分類"]]

    df_mega_final = df_all_grouped[df_all_grouped["來源"] == "兆豐"].rename(columns={
        "每日原始總分": "兆豐_每日原始總分",
        "左側情緒分類": "兆豐_左側情緒分類"
    })[["日期", "兆豐_每日原始總分", "兆豐_左側情緒分類"]]

    df_sentiment = pd.merge(df_cnyes_final, df_mega_final, on="日期", how="outer")

    # --- 加入 PTT 情緒處理 ---

    query = "SELECT * FROM 	ptt"
    ptt_df = pd.read_sql(query, engine)
    ptt_df.columns = ptt_df.columns.str.strip()

    positive_words_ptt = ['賺', '獲利', '上漲', '大漲', '看好', '漲停', '創新高', '買進', '反彈', '強勢']
    negative_words_ptt = ['虧', '虧損', '賠錢', '暴跌', '下跌', '崩盤', '跌停', '看壞', '停損', '賣壓']
    negation_words = ['不', '沒', '未', '無']

    def get_discrete_score(text):
        text = str(text)
        pos_hit = any(
            word in text and not any(n + word in text for n in negation_words)
            for word in positive_words_ptt
        )
        neg_hit = any(
            word in text and not any(n + word in text for n in negation_words)
            for word in negative_words_ptt
        )
        if pos_hit and not neg_hit:
            return 1
        elif neg_hit and not pos_hit:
            return -1
        else:
            return 0

    ptt_df["情緒分數"] = ptt_df["Title"].apply(get_discrete_score)
    ptt_df["日期"] = pd.to_datetime(ptt_df["Date"], errors="coerce").dt.date
    ptt_df = ptt_df.dropna(subset=["日期"])

    ptt_daily = (
        ptt_df.groupby("日期")["情緒分數"]
        .sum()
        .reset_index()
        .rename(columns={"情緒分數": "PTT_每日原始總分"})
    )
    ptt_daily["PTT_左側情緒分類"] = ptt_daily["PTT_每日原始總分"].apply(left_side_label)

    # --- 合併三方資料並輸出 ---
    df_sentiment = pd.merge(df_sentiment, ptt_daily, on="日期", how="outer").sort_values("日期")
    print("✅ 已整合鉅亨、兆豐、PTT")
    return df_sentiment
    