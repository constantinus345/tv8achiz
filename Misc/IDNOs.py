#& d:/Python_Code/tv8achiz/Scripts/python.exe d:/Python_Code/tv8achiz/Misc/IDNOs.py

import configs
import pymongo
from time import sleep
import pandas as pd
import DB_Create
import DB_Scripts


def df_idnos():
    #for document in myCollection.find():
    Mongo_client = pymongo.MongoClient(configs.MongoT_Client)
    Mongo_mydb = Mongo_client[configs.MongoT_Database]
    #Important: In MongoDB, a database is not created until it gets content!
    Mongo_mycol = Mongo_mydb[configs.MongoT_Collection]

    df_idnos = pd.DataFrame(columns=["idno", "institutia"])
    for doc in Mongo_mycol.find():
        try:
            try:
                Entity_IDNO = doc["releases"][0]["tender"]["procuringEntity"]["id"]
                Entity_Name = doc["releases"][0]["tender"]["procuringEntity"]["name"]
            except KeyError:
                Entity_IDNO = doc["releases"][0]["parties"][0]["id"]
                Entity_Name = doc["releases"][0]["parties"][0]["name"]



            df_idnos.loc[len(df_idnos)] = [Entity_IDNO, Entity_Name]
            if len(df_idnos) % 100 == 0:
                print(len(df_idnos))
        except Exception as e:
            print(e)
        

    df_idnos = df_idnos.drop_duplicates(keep= "last", ignore_index= True)
    return df_idnos


def IDNOs_toDB(): 
    DB_Create.create_table(Table_name= configs.Table_IDNOs, Column_List= configs.Columns_IDNOs)
    dfx= df_idnos()
    DB_Scripts.insert_df_data(dfx, table= configs.Table_IDNOs, if_exists="replace")

IDNOs_toDB()
print("Done")