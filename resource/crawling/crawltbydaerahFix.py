import json
import twint
from pymongo import MongoClient

import pandas as pd


class Scrapping:
    def __init__(self):
        pass

    def scrap(self, query):
        self.c = twint.Config()
        self.c.Since = '2017-01-01'
        self.c.Store_object = True
        self.c.Search = query
        twint.run.Search(self.c)
        self.tweets = twint.output.tweets_object
        return self.tweets


if __name__ == '__main__':
    count = 0
    conn = MongoClient('localhost', 27017, username='admin', password='12345') #ini perlu di sesuai in sama mongodb masing masing
    db = conn.Proyek5
    collection = db.test
    twit = []
    data_query = []
    data_per_wil = []
    data_per_wil.insert(0,0)
    dataB = pd.read_csv('list_query.csv', usecols=['Query Untuk Twint'])
    query_list = dataB['Query Untuk Twint'].tolist()
    dataB.info()
    print(query_list)

    for i in range(len(query_list)):
        sc = Scrapping()
        query = str(query_list[i]) + " and kemacetan from:infobdg"
        print(query)
        twit.append(sc.scrap(query))
        count = 0
        jml_query = 0
        cek = False
        for record in twit[i]:
            if count == data_per_wil[i]:
                cek = True

            if cek == True:        
                data = {"id": record.id, "conversation_id": record.conversation_id, "created_at": record.datetime, "date": record.datestamp, "time": record.timestamp, "timezone": record.timezone, "user_id": record.user_id, "username": record.username, "name": record.name, "place": record.place, "tweet": record.tweet, "mentions": record.mentions,
                    "urls": record.urls, "photos": record.photos, "replies_count": record.replies_count, "retweets_count": record.retweets_count, "likes_count": record.likes_count, "hashtags": record.hashtags, "cashtags": record.cashtags, "link": record.link, "retweet": record.retweet, "quote_url": record.quote_url, "video": record.video}
                collection.insert_one(data)
                jml_query+=1
            count += 1
        data_query.insert(i,jml_query)    
        data_per_wil.insert(i+1,count)
        jml_query = 0
        del sc
        

    print(data_query)
