import sys
import configs
import pymongo
import Mongo_misc
from time import sleep
import pandas as pd


def OCID_details(OCID):
    
    Mongo_client = pymongo.MongoClient(configs.MongoT_Client)
    Mongo_mydb = Mongo_client[configs.MongoT_Database]
    #Important: In MongoDB, a database is not created until it gets content!
    Mongo_mycol = Mongo_mydb[configs.MongoT_Collection]

    Count= Mongo_mycol.count_documents({"publishedDate" : { "$gte" : ("2012-01-12T20:15:31Z")}})
    #print(f"Len_docs ALL = {Count}")

    Errors = []
    try:
        MTender_Link = f"https://mtender.gov.md/tenders/{OCID}"
        #print(MTender_Link)
        Docx = Mongo_mycol.find_one({"releases.ocid":OCID})
        #print(Docx["publishedDate"])
        #Entitatea 
        #Denumirea 
        #IDNO
        Ent_OCID = Docx["releases"][0]["ocid"]
        Ent_Title = Docx["releases"][0]["title"]
        Ent_Published = Docx["publishedDate"]
        try:
            Ent_IDNO = Docx["releases"][0]["tender"]["procuringEntity"]["id"]
            Ent_Name = Docx["releases"][0]["tender"]["procuringEntity"]["name"]
        except:
            Ent_IDNO = Docx["releases"][0]["parties"][0]["id"]
            Ent_Name = Docx["releases"][0]["parties"][0]["name"]
        #Suma bugetata 
        try:
            Ent_Buget = Docx["releases"][0]["planning"]["budget"]["amount"]["amount"]
            Ent_Buget_Currency = Docx["releases"][0]["planning"]["budget"]["amount"]["currency"]
        except:
            Ent_Buget =          Docx["releases"][0]["tender"]["value"]["amount"]
            Ent_Buget_Currency = Docx["releases"][0]["tender"]["value"]["currency"]

        try:
            Ent_CPV = Docx["releases"][0]["tender"]["classification"]["scheme"] + Docx["releases"][0]["tender"]["classification"]["id"]
        except:
            Ent_CPV = ""
        #Data anunt 
        #Progres stage
        #Link MTender
        #Status
        Ent_Status = Docx["releases"][0]["tender"]["status"]
        Ent_ProcMethod = Docx["releases"][0]["tender"]["procurementMethod"]
        Ent_ProcMethod_Details = Docx["releases"][0]["tender"]["procurementMethodDetails"]
        Ent_ProcCateg = Docx["releases"][0]["tender"]["mainProcurementCategory"]
        

        #TenderPeriod
        Ent_Tender_StartDate = Docx["releases"][0]["tender"]["tenderPeriod"]["startDate"]
        try:
            Ent_Tender_EndDate = Docx["releases"][0]["tender"]["tenderPeriod"]["endDate"]
        except:
            Ent_Tender_EndDate = ""

        try:
            Ent_Award_StartDate = Docx["releases"][0]["tender"]["awardPeriod"]["startDate"]
        except KeyError:
            Ent_Award_StartDate = ""

        #Nr lots
        try:
            Ent_Lots = len(Docx["releases"][0]["tender"]["lots"])
        except KeyError:
            Ent_Lots = 1

        #https://mtender.gov.md/tenders/ocds-b3wdp1-MD-1550659281721?tab=awards
        #Suma alocata - awards-array where status= active awards[3][value][amount]/[currency] .. awards[3][suppliers-array][0][id/name]
        #% din suma
        #Companii Castigatoare
        Suma_Alocata_it = 0
        Lots_Awarded_it= 0
        Statuses = []
        Companies = []
        Awarded_active = 0

        try:
            Ent_Bids_Awards_Total = len(Docx["releases"][0]["awards"])
        except KeyError:
            Ent_Bids_Awards_Total = 0 

        if Ent_Bids_Awards_Total > 0:
            for doc in Docx["releases"][0]["awards"]:
                Statuses.append(doc["status"])
                if doc["status"] == "active":
                    #print(f"{'_'*60}")
                    Awarded_active += 1
                    #print(doc["status"])
                    try:
                        #print(doc["suppliers"][0]["id"])
                        #print(doc["suppliers"][0]["name"])
                        Companies.append(doc["suppliers"][0]["name"])
                        #print(doc["value"]["amount"])
                        Suma_Alocata_it += int(doc["value"]["amount"])
                        Lots_Awarded_it += 1
                        #print(doc["value"]["currency"])
                        #print(f"{'_'*60}")
                    except:
                        pass
        
        
        Ent_Awarded_Active = Awarded_active
        #print(Ent_Awarded_Active)
        Ent_Suma_Awarded = Suma_Alocata_it
        #print(Ent_Suma_Awarded)
        Ent_Lots_Awarded = Lots_Awarded_it
        Ent_Award_Statuses = ",".join(list(set(Statuses)))
        #print(Ent_Award_Statuses)
        #Companies list in a separate DB with cols= ocid, [awards][x][date], status, name, IDNO, sum (if awarded, status=active), currency
        Ent_Award_Companies_Active = ",".join(list(set(Companies)))
        try:
            Ent_Inquires = len(Docx["releases"][0]["tender"]["enquiries"])
        except KeyError:
            Ent_Inquires = 0
        
        try:
            Ent_Complaints = len(Docx["releases"][0]["reviewProceedings"]["complaints"])
        except KeyError:
            Ent_Complaints = 0

        try:
            Ent_Complaints_reviews = len(Docx["releases"][0]["reviewProceedings"]["reviews"])
        except KeyError:
            Ent_Complaints_reviews = 0


        List_Ent_Cols = configs.Columns_Procs_List
        List_Ent_Cols = [x.lower().replace("ent_","") for x in List_Ent_Cols]
        #print(len(List_Ent_Cols))
        #print(List_Ent_Cols[0])
        List_Ent_Values = [[Ent_OCID] ,[Ent_Title] , [Ent_Published], [Ent_IDNO], [Ent_Name], [Ent_Buget], [Ent_Buget_Currency],\
            [Ent_Status], [Ent_ProcMethod], [Ent_ProcMethod_Details], [Ent_ProcCateg], [Ent_CPV] ,\
                [Ent_Tender_StartDate], [Ent_Tender_EndDate], [Ent_Award_StartDate], [Ent_Lots], [Ent_Bids_Awards_Total], [Ent_Awarded_Active],\
                    [Ent_Suma_Awarded], [Ent_Lots_Awarded], [Ent_Award_Statuses], [Ent_Award_Companies_Active],\
                        [Ent_Complaints], [Ent_Complaints_reviews]]  
        
        dictx= dict(zip(List_Ent_Cols, List_Ent_Values))
        dfx = pd.DataFrame(dictx)
        #print(len(List_Ent_Values))
    except Exception as e:
        Errors.append([OCID, e])
        dfx = []
        #print(Errors)
    
    return {"Errors":Errors, "dfx":dfx}



if __name__ == "__main__":
    print("Executing the main")
else: 
    print(f"Imported {sys.argv[0]}")
