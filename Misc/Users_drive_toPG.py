from gspread import authorize
import sys
from oauth2client.service_account import ServiceAccountCredentials
from pandas import DataFrame as DF
import configs
import pandas as pd
from unidecode import unidecode as deaccent
from time import time, sleep
from Telegram_funcs import Send_Telegram_Message
import DB_Scripts
from difflib import SequenceMatcher as SM
from Telegram_funcs import Send_Telegram_Message

def df_drive(spreadsheet_key, type_sheet):

    JSON_Creds="noble-aquifer-315512-90a1b2abaf1c.json"

    scope = ['https://spreadsheets.google.com/feeds',
            'https://www.googleapis.com/auth/drive']

    #Reading Responses from https://forms.gle/YaViMoguTptoiaxY6
    credentials_drive = ServiceAccountCredentials.from_json_keyfile_name(JSON_Creds, scope) # Your json file here
    gc = authorize(credentials_drive)
    wks = gc.open_by_key(spreadsheet_key).get_worksheet(0)
    data = wks.get_all_values()
    headers = data.pop(0)
    df_User = DF(data, columns=headers)
    df_User.insert(0, "tip_subscription", type_sheet)
    """len1= len(df_User)
    Cod_Col="Codurile localitatilor"
    df_User = df_User[df_User[Cod_Col]!='']
    len2 = len(df_User)

    if len1!= len2:
        Text_Rep = f"You have {len1-len2} missing codes for {type_sheet}"
        Send_Telegram_Message(configs.Telegram_Constantin, Text_Rep)"""
    df_User.columns = configs.Users_Cols

    return df_User

dfx = df_drive(configs.drive_tv8regist_key, "achizitii_tv8")
df_idno = DB_Scripts.sql_readpd(configs.Table_IDNOs)
I_IDNO = df_idno["institutia"].tolist()
I_IDNO_IDNO = df_idno["idno"].tolist()
I_Users = dfx["institutia"].tolist()
I_Users_IDNO = dfx["idno"].tolist()

def process_fuzzy(strx):
    replace= [["primaria",""], ["municipiului",""]]
    strx = deaccent(strx).lower()
    strx = strx.split(",")[0]

    for rep in replace:
        strx = strx.replace(rep[0], rep[1])

    return strx

def Sort_subkey(sub_li, el=2):
    sub_li.sort(reverse=True, key = lambda x: x[el])
    return sub_li


#Biggest 3 ratios
def match_inst(strx, matches = 7):    
    Ratios = []
    for ii in I_IDNO:
        strxp = process_fuzzy(strx)
        iip = process_fuzzy(ii)
        ratiox = round(SM(None, strxp, iip).ratio(),2)
        if len(Ratios)>= matches:

            #Determine the smallest ratio from Ratios
            Ratios_only = [rat[2] for rat in Ratios]

            smallestr = min(Ratios_only)
            del Ratios[Ratios_only.index(smallestr)]
            #Determine if ratios is bigger than the smallest in Ratios, then
            #Delete the smallest 

            #insert new ratio
            Ratios.append([strx, ii, ratiox])

        else: 
            a=1 #append
            Ratios.append([strx, ii, ratiox])
            #print(len(Ratios))

    return Sort_subkey(Ratios)

def insert_registers_tv8_todb():
    for usr in range(len(I_Users)):


        if len(I_Users_IDNO[usr])> 100:
            continue
        print(f"{'_'*60}\n>>{usr+2}<<\n")
        for el in match_inst(I_Users[usr]):
            el.append(I_IDNO_IDNO[I_IDNO.index(el[1])])
            print(el)


    dfx = df_drive(configs.drive_tv8regist_key, "achizitii_tv8")
    no_idno = len([x for x in dfx["idno"].tolist() if len(x)<5])
    if no_idno >0 :
        link_registers = "https://docs.google.com/spreadsheets/d/1Jznhk24KumVhCeE-nk1S9Dx1r7i3p1gA3yAg95b6vhE/edit#gid=142613098"
        Send_Telegram_Message(configs.Telegram_Constantin, f"In reponses TV8 there are {no_idno} IDNOs missing, \n\n{link_registers}")

    DB_Scripts.insert_df_data(dfx, configs.Users_Table, if_exists="replace")

if __name__ == "__main__":
    print("Executing the main")
    insert_registers_tv8_todb()
else: 
    print(f"Imported {sys.argv[0]}")
