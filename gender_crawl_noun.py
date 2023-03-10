import pandas as pd
import requests
import mysql.connector
import pymongo
import time
import threading
import os
import json
class noun:
    def __init__(self,user, password, host, database, url_connect_mongo):
        #connect mysql
        print("file None, create content file")
        self.cnx = mysql.connector.connect(user=user, password=password,
                            host=host,
                            database=database)
        self.df=pd.read_sql("SELECT id, form, lemma FROM nomen", self.cnx)
        self.noun=set(self.df['form'])
        print(len(self.noun))
        # lưu tổng số request ra file
        with open("request.txt","w",encoding='utf-8')  as file:
            for n in self.noun:
                file.write(n+"\n")
            
        print(type(noun))
        # mongodb+srv://nguyenconghau:258000@cluster0.kmhzq5p.mongodb.net/?retryWrites=true&w=majority
        self.client = pymongo.MongoClient(url_connect_mongo)
        self.db=self.client['fazzta']
        self.collection=self.db['noun']
        self.remaining=self.noun
# hàm crawl
    def crawl(self,noun):
        print(f"noun: {noun}")
        key=[noun]
        self.remaining=self.remaining-set(key)
        try:
            request={}
            url="https://www.qmez.de:8444/v1/scanner/es/s?w="+str(noun)
            response=requests.get(url)
            if response.text=="":
                print(f"failed")
                request['key']=noun
                request['request']=''
                # request['word_recomment']=noun
                query=request['status']='failed'
                self.collection.insert_one(request)
                return(f"commit comlete {query}")
            js_gender=response.json()
            request['key']=noun
            request['request']= js_gender
            # request['word_recomment']=word
            request['status']="success"
            
            query=self.collection.insert_one(request)
            print(f"******success commit comlete {query}")
        except:
            with open("request.txt", "w",encoding="utf-8") as f:
                for k in self.remaining:
                    f.write(k+"\n")
        # print()
        # self.genders_ls.append(js_gender)
        time.sleep(3)
        
    def threading(self):
        thread=[]
        o=0
        for n in self.noun:
            print(o)
            o+=1
            t=threading.Thread(target=self.crawl,args=(n,))
            thread.append(t)
            t.start()

        for t in thread:
            t.join()

gender=noun(user='root', password='258000',host='localhost',database='fazzta', url_connect_mongo="mongodb+srv://nguyenconghau:258000@cluster0.kmhzq5p.mongodb.net/?retryWrites=true&w=majority")
# gender.crawl("Geologennew")
gender.threading()