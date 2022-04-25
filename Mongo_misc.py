#{"releases.tender.status": "active" }
import configs
import pymongo
from time import sleep
import pandas as pd
import sys


def Statuses():
    #for document in myCollection.find():
    Mongo_client = pymongo.MongoClient(configs.MongoT_Client)
    Mongo_mydb = Mongo_client[configs.MongoT_Database]
    #Important: In MongoDB, a database is not created until it gets content!
    Mongo_mycol = Mongo_mydb[configs.MongoT_Collection]

    Count= Mongo_mycol.count_documents({"publishedDate" : { "$gte" : ("2012-01-12T20:15:31Z")}})
    #print(f"Len_docs ALL= {Count}")

    Status = []
    for doc in Mongo_mycol.find():
        try:
            status = doc["releases"][0]["tender"]["status"]
            Status.append(status)
        except Exception as e:
            print(e)
        

    Status = list(set(Status))
    return Status

#print(Statuses())

#Gets OCIDs from Mongo
def OCID_List(Date_Str):
    #for document in myCollection.find():
    Mongo_client = pymongo.MongoClient(configs.MongoT_Client)
    Mongo_mydb = Mongo_client[configs.MongoT_Database]
    #Important: In MongoDB, a database is not created until it gets content!
    Mongo_mycol = Mongo_mydb[configs.MongoT_Collection]

    Count= Mongo_mycol.count_documents({"publishedDate" : { "$gte" : (Date_Str)}})
    print(f"Len_docs after {Date_Str[:10]} = {Count}")
    
    OCIDs = []
    for doc in Mongo_mycol.find({"publishedDate" : { "$gte" : (Date_Str)}}):
        try:
            rel = doc["releases"]
            if len(rel)>1:
                print(rel[0]["ocid"])
            else:
                OCIDs.append(rel[0]["ocid"])

        except Exception as e:
            print(e)
    
    return OCIDs


if __name__ == "__main__":
    print("Executing the main")
else: 
    print(f"Imported {sys.argv[0]}")
