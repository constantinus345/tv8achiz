import SG_Scripts
import DB_Scripts
import configs
import Users_to_Update
from time import time, sleep
import pandas as pd
from random import randint as rdm
from Telegram_funcs import Send_Telegram_Message
time_start = time()

EMAILS_ALL = Users_to_Update.get_user_allemails()
Template_id = configs.SG_Templates[configs.announce_tip_mtawarded]
Unsub_id = configs.SG_Unsub_Groups[configs.announce_tip_mtawarded]
groups_to_unsub = configs.Unsub_ids

Counter_Emails = 0
Counter_Users = []
Counter_OCIDs = []

for email in EMAILS_ALL:
    if len(Users_to_Update.get_user_idnos(email))==0: continue
    #if EMAILS_ALL.index(email)!= 1: continue
    try:
        dfx = Users_to_Update.get_user_details(email).reset_index()
    except: 
        dfx= []
    if len(dfx)==0: continue
    dfa_unixtimestamp = int(time())
    dfa_user_name = dfx["user_fullname"][0]
    dfa_raion = dfx["raion"][0]
    dfa_announce_tip = configs.announce_tip_mtawarded
    dfa_channel_type = "email"
    for idno in Users_to_Update.get_user_idnos(email):
        IDNO_Data = Users_to_Update.get_dataload_to_announce_mtawarded(email, idno)
        if len(IDNO_Data)==0 :continue

        for idx in range(len(IDNO_Data)):
            
            #print(IDNO_Data.columns.tolist())
            Inst_Name = IDNO_Data["name"][idx]
            Inst_IDNO = IDNO_Data["idno"][idx]
            OCID_ocid = IDNO_Data["ocid"][idx]
            OCID_Title = IDNO_Data["title"][idx]
            OCID_Awar = str(IDNO_Data["award_startdate"][idx])[:10]
            OCID_Buget = str(IDNO_Data["buget"][idx])
            OCID_Currency = IDNO_Data["buget_currency"][idx]
            OCID_Lots = str(IDNO_Data["lots"][idx])
            OCID_Bids = str(IDNO_Data["bids_awards_total"][idx])

            OCID_Buget_Dot = str(Users_to_Update.int_format(int(OCID_Buget)))


            template_dict = {"user_name":dfa_user_name,"Inst_Name":Inst_Name, "Inst_IDNO":Inst_IDNO,
                "OCID_ocid":OCID_ocid, "OCID_Title":OCID_Title, "OCID_Awar":OCID_Awar,
                "OCID_Buget":OCID_Buget_Dot, "OCID_Currency":OCID_Currency, 
                "OCID_Lots":OCID_Lots, "OCID_Bids":OCID_Bids}


            Users_announced_Values = [[dfa_unixtimestamp], [dfa_user_name] , [email], [Inst_Name],\
            [dfa_raion],[Inst_IDNO],[dfa_announce_tip],[OCID_ocid],[OCID_Awar],[" "],\
            [OCID_Buget],[OCID_Title],["active"],[0], [dfa_channel_type], [email]]
            dictx= dict(zip(configs.Users_announced_Cols, Users_announced_Values))

            #TO_email = "constantin.copaceanu@gmail.com"
            TO_email = email
            pdf_paths = []

            SG_Scripts.Send_SG_with_attachment(TO_email, Template_id, pdf_paths, Unsub_id, groups_to_unsub,
            template_dict,
            FROM_email= "constantin@implicareplus.org", FROM_name = "Implicare Plus",
            SG_API_Key= configs.SGK1)
            
            Counter_Emails += 1
            Counter_Users.append(email)
            Counter_OCIDs.append(OCID_ocid)
            
            dfa = pd.DataFrame(dictx)
            DB_Scripts.insert_df_data(dfa, configs.Users_announced_Table)
            print(f"{'_'*60}")
            sleep(rdm(1,3))


Send_Telegram_Message(configs.Telegram_Constantin,\
    f"TV8 Achizitii> sent {Counter_Emails} Awarded emails, \
    to {len(set(Counter_Users))} users about {len(set(Counter_OCIDs))} ocids".replace("  ",""))


time_end = time()
took = round(time_end- time_start, 2)
print(f"Took {took} seconds")