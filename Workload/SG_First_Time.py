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

Template_id = configs.SG_Templates[configs.announce_tip_mtfirst]

Unsub_id = configs.SG_Unsub_Groups[configs.announce_tip_mtfirst]
print(Unsub_id)

groups_to_unsub = configs.Unsub_ids

"""Users_Cols = ['tip_subscription', 'timestamp', 'institutia', 'idno', 'telefon', 'announce_tip', 'email', 'user_fullname', 'raion']

Users_announced_Cols = [ 'unixtimestamp', 'user_name', 'user_email', 'institutia',\
                 'raion', 'idno', 'announce_tip', 'ocid', 'date_published' , 'date_award', \
                     'buget' , 'name_achizitie', 'status', 'lots' , 'channel_type', 'channel_value']
#insert_df_data(dfx, configs.Table_Users_Announced)"""

Counter_Emails = 0
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
    dfa_announce_tip = configs.announce_tip_mtfirst
    dfa_channel_type = "email"

    for idno in Users_to_Update.get_user_idnos(email):
        IDNO_Data = Users_to_Update.get_dataload_to_announce_mtfirst(email,idno)
        print(IDNO_Data)
        if len(IDNO_Data)==0 : continue
        try:
            if len(IDNO_Data[0])==0 :continue
        except:
            pass

        template_dict = {"user_name":dfa_user_name,"Inst_Name":str(IDNO_Data[1]), "Inst_IDNO":str(IDNO_Data[0].split("-")[-1]),
        "Inst_Buget": str(Users_to_Update.int_format(IDNO_Data[2])), 
        "Inst_Count":str(IDNO_Data[3]), "Inst_Lots":str(IDNO_Data[4]),"Inst_compl":str(IDNO_Data[5])}
        print(template_dict)

        Users_announced_Values = [[dfa_unixtimestamp], [dfa_user_name] , [email], [IDNO_Data[1]],\
        [dfa_raion],[IDNO_Data[0]],[dfa_announce_tip],[" "],[" "],[" "],\
        [IDNO_Data[2]],[" "],[" "],[IDNO_Data[4]], [dfa_channel_type], [email]]
        dictx= dict(zip(configs.Users_announced_Cols, Users_announced_Values))


        TO_email = email
        pdf_paths = []

        SG_Scripts.Send_SG_with_attachment(TO_email, Template_id, pdf_paths, Unsub_id, groups_to_unsub,
        template_dict,
        FROM_email= "constantin@implicareplus.org", FROM_name = "Implicare Plus",
        SG_API_Key= configs.SGK1)

        dfa = pd.DataFrame(dictx)
        DB_Scripts.insert_df_data(dfa, configs.Users_announced_Table)
        Counter_Emails+=1
        print(f"{'_'*60}")
        sleep(rdm(1,3))

Send_Telegram_Message(configs.Telegram_Constantin, f"TV8 Achizitii> sent {Counter_Emails} First Emails")

time_end = time()
took = round(time_end- time_start, 2)
print(f"Took {took} seconds")