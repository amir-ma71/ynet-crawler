import re
import requests
from bs4 import BeautifulSoup
import pandas as pd
import time

#       ****  CRAWl ******
# ***** israelhayom.co.il/ *********
#  get RSS bof this site, it runs every day

# -* request settings *-
headers = {'user-agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                         "(KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36"}
proxyDict = {"https": "10.253.38.1:808"}

# load last update of date in rss.date
rss_name_df = pd.read_csv("rss.date")
rss_name_df = rss_name_df.sample(frac=1)
rss_name_dict = dict([(i, a) for i, a in zip(rss_name_df.name, rss_name_df.date)])  # convert to dict from dataframe

print(" ************* this is Ynet site ***********")
counterDATE = 0
counterLINK = 1
counterRSS = 1
counterNOTEXT = 1
while True:
    for rss_name in rss_name_dict.keys():
        try:
            # request RSS from dict
            rss = "http://www.ynet.co.il/Integration/" + rss_name
            rss_selected = requests.get(rss,
                                        proxies=proxyDict,
                                        headers=headers)
        except:
            print("request ERROR . . .")
            continue
        soup_rss_selected = BeautifulSoup(rss_selected.content.decode(), 'xml')

        last_Date_old_df = rss_name_dict[
            rss_name]  # set last date in file inplace of of old date to compare between now date
        last_Date_old_df_changed = time.strptime(last_Date_old_df, "%a, %d %b %Y  %H:%M:%S")

        if len(soup_rss_selected.findAll("item")) != 0:
            print(rss_name, " loaded . . .!")
            for link_index in range(1, len(soup_rss_selected.findAll("item")) + 1):
                crawled_data = pd.DataFrame()  # define empty dataframe

                now_news_date = soup_rss_selected.findAll("item")[-link_index].find("pubDate").contents[0].split("+")[
                    0].strip()  # get the oldest news in rss
                now_news_date_changed = time.strptime(now_news_date, "%a, %d %b %Y  %H:%M:%S")

                rss_name_dict[rss_name] = now_news_date  # update old date in rss.date
                counterDATE += 1
                df = pd.DataFrame(rss_name_dict.items(), columns=["name", "date"])  # save the update
                df.to_csv("rss.date", index=False)

                if now_news_date_changed > last_Date_old_df_changed:
                    link = soup_rss_selected.findAll("item")[-link_index].find("link").contents[0]
                    try:
                        des = \
                        soup_rss_selected.findAll("item")[-link_index].find("description").contents[0].split("div")[2]
                        des = re.sub('[<=-_>,;%/~"]', '', des)
                        des = re.sub('\n', '', des).strip()
                        des = re.sub('\t', '', des).strip()
                    except:
                        des = ""

                    title = soup_rss_selected.findAll("item")[-link_index].find("title").contents[0]
                    title = re.sub('[~"]', '', title)
                    title = re.sub('\n', '', title).strip()
                    title = re.sub('\t', '', title).strip()
                    # des = str(soup_rss_selected.findAll("item")[link_index].contents[3].contents[1])
                    category = link.split("/")[3]
                    time.sleep(5)
                    site_selected = requests.get(link,
                                                 proxies=proxyDict,
                                                 headers=headers,
                                                 )

                    soup_selected = BeautifulSoup(site_selected.content, 'html.parser')

                    text_list = soup_selected.findAll("div", attrs={"data-contents": "true"})
                    if len(text_list) != 0:
                        text = ""
                        for txt in text_list[0].contents:
                            try:
                                if txt.attrs["class"][0] == 'text_editor_paragraph':
                                    text = text + txt.find("span").contents[0].contents[0] + " "
                            except:
                                continue
                        text = re.sub('[<=-_>,;%/~"]', '', text)
                        text = re.sub('\n', '', text).strip()
                        text = re.sub('\t', '', text).strip()
                    else:
                        print("notext ----->", link)
                        continue
                    # fill the empty df to add the old data.csv
                    crawled_data["title"] = [title]
                    crawled_data["description"] = [des]
                    crawled_data["text"] = [text]
                    crawled_data["category"] = [category]
                    crawled_data["date"] = [now_news_date]
                    crawled_data["link"] = [link]

                    crawled_data.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False, mode="a",
                                        header=False)
                    print(counterLINK, " link crawled Done!")
                    counterLINK += 1

                else:
                    print("before detected")
                    continue
        else:
            print("not item on RSS", rss_name)

        time.sleep(30)

    df = pd.read_csv("data.csv", quoting=1, sep="~", encoding="utf-8")
    df = df.drop_duplicates(subset=["link"])
    df.to_csv("data.csv", quoting=1, encoding="utf-8", sep="~", index=False)
    del df

    time.sleep(1800)
