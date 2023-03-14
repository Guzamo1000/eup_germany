import pandas as pd
import spacy
import pymongo
import time
import threading

class noun_identify:
    def __init__(self,api_mongodb):
        self.client=pymongo.MongoClient(api_mongodb)
        self.db=self.client['fazzta']
        self.collection=self.db['noun2']

    def identify_nouns(self,noun):
        nlp = spacy.load("de_core_news_sm")
        
        doc = nlp(noun)

        for token in doc:
            if "NOUN" in token.pos_:
                if "Number=Plur" in token.morph:
                    return "plural"
                else:
                    return "singular"
    def main(self):
        # noun=[]
        data=self.collection.find()
        for dt in data:
            noun=dt['key']
            number_of_noun=self.identify_nouns(list(noun))
            self.collection.update_one({"key":noun},{"$set":{"quantity":number_of_noun}})
            print(f"success {noun}")
    def threading(self):
        thread=[]
        o=0
        noun=self.collection.find()
        for n in noun:
            print(o)
            o+=1
            t=threading.Thread(target=self.identify_nouns, args=(n,))
            thread.append(t)
            t.start()
        for t in thread:
            t.joint()

t=noun_identify("mongodb+srv://nguyenconghau:258000@cluster0.kmhzq5p.mongodb.net/?retryWrites=true&w=majority")
t.threading()