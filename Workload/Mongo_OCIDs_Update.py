#& d:/Python_Code/tv8achiz/Scripts/python.exe d:/Python_Code/tv8achiz/Workload/Mongo_OCIDs_Update.py
#This would be first, followed by Mongo to PG
from time import time
tsart= time()
import configs
import pymongo
from time import sleep
import requests
import sys
import Telegram_funcs


Mongo_client = pymongo.MongoClient(configs.MongoT_Client)
Mongo_mydb = Mongo_client[configs.MongoT_Database]
Mongo_mycol = Mongo_mydb[configs.MongoT_Collection]

def last_offset():
    #Get last offset
    docx = Mongo_mycol.find().sort("publishedDate",-1).limit(1)

    offset= docx[0]["publishedDate"]
    return offset



def offset_minus_1year(Offset_Date):
    
    Offset_Start_1Year = int(Offset_Date[:4])-1
    Offset_minus_1year = f"{Offset_Start_1Year}{Offset_Date[4:]}"
    return Offset_minus_1year


#Gets OCIDs from API
def get_ocids(Offset):
    Page_OCDS_ALL="https://public.mtender.gov.md/tenders"
    OCIDs = []
    while True:
        Page_OCDS = requests.get(f"{Page_OCDS_ALL}?offset={Offset}").json()
        if len(Page_OCDS)>0:
            OCIDs.extend(Page_OCDS["data"])
        else:
            break 

        if Page_OCDS["offset"] == Offset:
            break
        Offset = Page_OCDS["offset"]
        print(Offset)
    return OCIDs


def insert_ocids(OCIDs, indexx= 0):

    
    for doc in OCIDs:
        if OCIDs.index(doc) < indexx:
            continue

        OCDS_gen= "http://public.eprocurement.systems/ocds/tenders/"

        OCIDx_data= requests.get(f"{OCDS_gen}{doc['ocid']}").json()
        #Deletes Existing doc and hence inserts the updated one.
        try:
            query = {"uri": OCIDx_data["uri"] } 
            Mongo_mycol.delete_one(query)
            Mongo_mycol.insert_one (OCIDx_data)
        except KeyError:
            pass

        if OCIDs.index(doc)%20 == 0:
            x= last_offset()
            print(f"latest date = {x} | index= {OCIDs.index(doc)}/ {len(OCIDs)}")


def main():
    #Offset_Start = last_offset()
    #Offset_Start = configs.Offset_First
    Offset_Start = offset_minus_1year(last_offset())
    OCIDs = get_ocids(Offset_Start)
    print(len(OCIDs))
    insert_ocids(OCIDs)

    took = int(time() -tsart)
    Telegram_funcs.Send_Telegram_Message(configs.Telegram_Constantin, f"(tv8achiz) Updated OCIDs to Mongo. Took {took} sec for {len(OCIDs)} OCIDs.")

if __name__ == "__main__":
    print("Executing the main")
    main()
else: 
    print(f"Imported {sys.argv[0]}")
