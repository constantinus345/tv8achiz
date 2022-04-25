"""
& d:/Python_Code/ActeLocale/Scripts/python.exe d:/Python_Code/ActeLocale/SG_test_email.py
"""
import random
import configs
from SG_funcs import Send_SG_with_attachment
from unidecode import unidecode as utf
from time import time, sleep

from DB_funcs import codes_users_unique, email_registered, urls_announced,\
    useremail_url_aplcod_check_channel_announcement, user_registered_dict,\
    usersEmails_announced , data_rsal_from_url, insert_df_data
    
from USERS_DFs_forms import return_last_by_keyword_list_urls,\
     APL_and_Codes,  report_announced

from Telegram_funcs import Send_Telegram_Message, Send_Telegram_Attachments
from random import randint as rdm

#____________________________________________
Coduri_Users_Unique = codes_users_unique()
Email_Registered_Unique = email_registered()


dict_APLs = APL_and_Codes()

FROM_email= "constantin@implicareplus.org"
FROM_name = "Implicare Plus"
SG_API_Key= configs.SGK1


APL_List_all = [x.split(",")[0] for x in dict_APLs["APLs"]]
APL_Code_all = dict_APLs["Codes"]

print("Lens of APL code and names= ",len(APL_List_all), len(APL_Code_all))

def aplname_from_code(code):
    try:
        cod_index= APL_Code_all.index(code)
        APL_name = APL_List_all[cod_index]
    except ValueError:
        APL_name = "_"
    return APL_name

Template_ids= configs.Template_ids
Unsub_ids= configs.Unsub_ids
Topics= configs.Topics
#Topics= ["sedin","buget", "licita", "achiz"]
Filters = configs.Filters
groups_to_unsub = Unsub_ids

def decision_send(topic_index):

    Report_Emails = 0
    Report_Telegram = 0
    Report_Telegram_Inregistrare = 0

    Template_id = Template_ids[topic_index]
    Unsub_id = Unsub_ids[topic_index]


    for user_email in Email_Registered_Unique:
        #testing as "constantin.copaceanu@gmail.com"
        #if user_email != "constantin.copaceanu@gmail.com":
         #   continue
        user_interests= user_registered_dict(user_email)

        for cod in Coduri_Users_Unique:
            
            #iterate over user emails registered and check if they are interested in the cod
            #If user not interested in the code, continue looping
            condition_cod = cod in user_interests["codes"]
            if not condition_cod:
                continue
            
            #If user not interested in the topic, continue looping
            condition_topic = Topics[topic_index] in utf(user_interests["topics"].lower())
            if not condition_topic:
                continue

            #If emails were sent of the return_last_by_keyword_list_urls
            List_URLs_OF_interest = return_last_by_keyword_list_urls(cod, Filters[topic_index], limit= 200)
            for rsal_url_interest in List_URLs_OF_interest:
                dfx= 0 
                
                email_url_aplcod_announced_check = useremail_url_aplcod_check_channel_announcement(user_email, rsal_url_interest , aplcod=cod, channel_type="email")
                #IF URL was not sent, check if user wants email
                if not email_url_aplcod_announced_check:

                    #If user IS interested in email, send email
                    condition_channel_email = "email" in utf(user_interests["channel"].lower())
                    if condition_channel_email:

                        #>>>Variables

                        TO_email = user_email
                        TO_name= user_interests["name"]
                        #TODO include a download pdf, and a try in case files do not exist. Also only one file if multiple.

                        APL_Name_0 = aplname_from_code(str(cod))
                        
                        URL_decizie = rsal_url_interest
                        disp_data = data_rsal_from_url(rsal_url_interest)
                        
                        disp_name = disp_data["disp_name"]
                        data_disp = disp_data["data_disp"]
                        pdf_paths= disp_data["pdf_dw_list"]
                        
                        cod_apl= cod
                        APL_Page= f"https://actelocale.gov.md/ral/search?apl={cod_apl}"

                        template_dict_sb= {"name":TO_name,
                        "Primaria_split0": APL_Name_0,
                        "Disp_name": disp_name,
                        "data_disp": data_disp, 
                        "url_decizie": URL_decizie,
                        "APL_Page": APL_Page
                        }
                        Send_SG_with_attachment(TO_email, Template_id, pdf_paths, Unsub_id, groups_to_unsub, template_dict = template_dict_sb)
                        
                        print(f"Sent email to {user_email} ({Email_Registered_Unique.index(user_email)}/{len(Email_Registered_Unique)})  for {cod}\n{disp_name}")

                        unixtimestamp = int(time())
                        intcod=int(cod)
                        dfx = report_announced(unixtimestamp , TO_name ,APL_Name_0 ,intcod , user_email, rsal_url_interest, "email", user_email)
                        
                        
                        insert_df_data(dfx, configs.Table_Users_Announced)
                        Report_Emails += 1




                telegram_url_aplcod_announced_check = useremail_url_aplcod_check_channel_announcement(user_email, rsal_url_interest , aplcod=cod, channel_type="telegram")
                #IF URL was not sent, send it 
                if not telegram_url_aplcod_announced_check:




                    #If user IS interested in telegram and has telegramid, send telegram
                    condition_channel_telegram = "telegram" in utf(user_interests["channel"].lower())
                    condition_has_telegramid = int(user_interests["telegramid"]) > 10
                    
                    if condition_channel_telegram and condition_has_telegramid:
                        #>>>Variables

                        APL_Name_0 = aplname_from_code(str(cod))
                        URL_decizie = rsal_url_interest

                        disp_data = data_rsal_from_url(rsal_url_interest)
                        disp_name = disp_data["disp_name"]
                        data_disp = disp_data["data_disp"]
                        pdf_paths= disp_data["pdf_dw_list"]
                        
                        cod_apl= cod
                        APL_Page= f"https://actelocale.gov.md/ral/search?apl={cod_apl}"

                        TelegramID = int(user_interests["telegramid"])
                        Text= f"{APL_Name_0}:\n{disp_name} din {data_disp}"
                        #Send_Telegram_Message(configs.Telegram_Constantin, Text)
                        try:
                            Send_Telegram_Message(TelegramID, Text)
                            Send_Telegram_Attachments(TelegramID, pdf_paths)

                            unixtimestamp = int(time())
                            intcod=int(cod)
                            TO_name= user_interests["name"]
                            TelegramID_str = user_interests["telegramid"]
                            dfx = report_announced(unixtimestamp , TO_name, APL_Name_0, intcod, user_email, rsal_url_interest, "telegram", TelegramID_str)
                            
                            insert_df_data(dfx, configs.Table_Users_Announced)
                            Report_Telegram += 1

                            print(f"Sent Telegram to {TO_name} and registered it")
                            sleep(rdm(1,2))
                            
                            
                            nr_rdm = random.randint(1,40)
                            if nr_rdm == 3:
                                Link_formular = "https://forms.gle/hWmVfkMRXHon5SYM6"
                                Text= f"Spuneți și prietenilor să se înregistreze pentru a fi informați despre primăria lor:\n\n{Link_formular}\n\nUn proiect al A.O. Implicare Plus\nconstantin@implicareplus.org :)"
                                Send_Telegram_Message(TelegramID, Text)
                                Report_Telegram_Inregistrare += 1
                        except Exception as e:
                            print(e)




                #condition_channel_sms = "sms" in utf(user_interests["channel"].lower())
    

    Report_Text = f"Sent {Report_Emails} emails, {Report_Telegram} telegram, {Report_Telegram_Inregistrare} telegram promo about >>{Topics[topic_index]}<<"
    Send_Telegram_Message(configs.Telegram_Constantin, Report_Text)


