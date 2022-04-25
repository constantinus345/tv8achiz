# & d:/Python_Code/tv8achiz/Scripts/python.exe d:/Python_Code/tv8achiz/Workload/Mongo_to_PG_BigTable.py
from time import time
tsart= time()
import sys
import Mongo_IDNO_Funcs
import Mongo_OCIDs_Update
import DB_Scripts
import Mongo_misc
import configs
from Telegram_funcs import Send_Telegram_Message


#Datex = "2012-01-12T20:15:31Z"
#Offset_Start = last_offset()
#Offset_Start = configs.Offset_First
#Gets the last date in MongoDB minus one year
Offset_Start = Mongo_OCIDs_Update.offset_minus_1year(Mongo_OCIDs_Update.last_offset())


#OCIDs_List_ALL = Mongo_misc.OCID_List(Datex)
OCIDs_List_ALL = Mongo_misc.OCID_List(Offset_Start)
Errors = []

for OCID in OCIDs_List_ALL:
    #if OCIDs_List_ALL.index(OCID) <= 1346:
     #   continue
    dictx = Mongo_IDNO_Funcs.OCID_details(OCID)
    dfx= dictx["dfx"]
    if len(dfx) < 1:
        print(OCID)
        print(dictx["Errors"])
    else:
        DB_Scripts.insert_df_data(dfx, configs.Table_Procs)

    if OCIDs_List_ALL.index(OCID) % 50 == 0:
        print(f"Done {OCIDs_List_ALL.index(OCID)} / {len(OCIDs_List_ALL)}")
    Errors.extend(Mongo_IDNO_Funcs.OCID_details(OCID)["Errors"])

print(Errors)

took = int(time() -tsart)

Send_Telegram_Message(configs.Telegram_Constantin, f"(tv8achiz) Updated OCIDs to Mongo. Took {took} sec for {len(OCIDs_List_ALL)} OCIDs.\n\nErrors = {Errors}")


if __name__ == "__main__":
    print("Executing the main")
else: 
    print(f"Imported {sys.argv[0]}")
