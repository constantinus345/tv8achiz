#Create DB with users announced 
#Create function with selecting what was announced
#Creare function with selecting what to announce, some sort of join and if not in. 

import pandas as pd
import datetime as dt
import DB_Scripts
import configs
import random

#Read users
#Read users_announced
#Read procurements

df_users = DB_Scripts.sql_readpd(configs.Users_Table)
df_anoun = DB_Scripts.sql_readpd(configs.Users_announced_Table)
df_procu = DB_Scripts.sql_readpd(configs.Table_Procs)

def flatten_list(t):
    return [item for sublist in t for item in sublist]

def int_format(ix):
    return f"{ix:,}".replace(",",".")

def get_user_allemails():
    #Consider the duplicate emails
    dfx= df_users["email"].tolist()
    emails = [x.split(",") for x in dfx]
    return list(dict.fromkeys(flatten_list(emails)))

#print(get_user_allemails())

def get_user_details(email):
    dfx= df_users[df_users["email"].str.contains(email, case=False)]
    return dfx


def get_user_idnos(email):
    #Consider the duplicate emails
    dfx= df_users[df_users["email"].str.contains(email, case=False)]
    if len(dfx)>0:
        idnos = [x.split(",") for x in dfx["idno"].tolist()]
        idnox = list(dict.fromkeys(flatten_list(idnos)))
        idnos = [int(x) for x in idnox]
        return idnos
    return []

def get_user_interest_all_ocids(email):
    idnos_query= [f"%%{x}%%" for x in get_user_idnos(email)]
    query = f"""
            SELECT *
            FROM {configs.Table_Procs} prc
            where idno  like any (ARRAY {idnos_query})
            order by published desc
            """
    dfx = DB_Scripts.sql_readpd_custom(configs.Table_Procs, query)
    dfx.drop_duplicates(subset = "ocid", keep= "last", inplace= True)
    return dfx


def get_user_latest_ocids_idno(email, idnox,awarded_bool, statut ="active", limit=2, daysdelta = 1, weeksdelta = 6):
    datex_big = dt.datetime.now() - dt.timedelta(days= daysdelta)
    datex_small = dt.datetime.now() - dt.timedelta(weeks= weeksdelta)
    if awarded_bool:
        awarded_check = "> 7"
    else:
        awarded_check = "< 7"

    if statut != "active":
        statut_array = [f"%%{x}%%".replace(" ","") for x in statut.split(",")]
        statut = f" LIKE ANY (ARRAY {statut_array})"
    else:
        statut = " = 'active'"

    query = f"""

            SELECT *
            FROM {configs.Table_Procs} prc
            where prc.idno  like '%%{idnox}%%'
            and published < '{datex_big}' 
            and published > '{datex_small}'
            AND length(prc.award_startdate) {awarded_check}
            and prc.status {statut}
            order by published desc
            LIMIT {limit}
            """

    #print(query)
    dfx = DB_Scripts.sql_readpd_custom(configs.Table_Procs, query)
    #keep last must be, for the updated version of the same ocid = published in the same date. 
    dfx.drop_duplicates(subset = "ocid", keep= "last", inplace= True)
    return dfx

def iname_tbuget_count_avglots_compl_idno_year(idnox, years=1, daysdelta = 1, weeksdelta = 53):
    #Returns sum_bugetat, nr tenders, avg_lots, 
    datex_big = dt.datetime.now() - dt.timedelta(days= daysdelta)
    datex_small = dt.datetime.now() - dt.timedelta(weeks= weeksdelta)
    query = f"""
            SELECT distinct unaccent(upper(prc.name)) as iname, idno, sum(buget)::numeric::integer as sum_b, count(buget)::numeric::integer, ROUND(AVG(lots),2) as avg_lots, sum(complaints) as sum_c
            FROM {configs.Table_Procs} prc
            where prc.idno  like '%%{idnox}%%'
            AND length(prc.award_startdate)>7
            and prc.published < '{datex_big}' 
            and prc.published > '{datex_small}'
            AND buget_currency = 'MDL'
            AND status = 'active'
            GROUP BY iname, idno
            """
    #print(query)
    dfx = DB_Scripts.sql_readpd_custom(configs.Table_Procs, query)
    
    if len(dfx)>0:
        return dfx["idno"][0], dfx["iname"][0], int(dfx["sum_b"][0]), dfx["count"][0], dfx["avg_lots"][0], dfx["sum_c"][0]
    return []

def get_dataload_to_announce_mtfirst(email, idnox):
    #linked to get_user_interest
    #linked to user_announcedDB
    #IF ema

    #Variables= Name_Inst, IDNO, tenders, suma, average_lots, total_complaints

    User_List_announce_first= []

    query = f"""
            SELECT user_email, announce_tip
            FROM public.users_announced prc
            WHERE user_email = '{email}'
            AND announce_tip = '{configs.announce_tip_mtfirst}'
            ORDER BY id ASC 
            """
    dfx = DB_Scripts.sql_readpd_custom(configs.Users_announced_Table , query)

    if len(dfx) == 0:
        User_List_announce_first.append(iname_tbuget_count_avglots_compl_idno_year(idnox))

    return User_List_announce_first

print(iname_tbuget_count_avglots_compl_idno_year(1006601001012))
"""
ALL_Emails = get_user_allemails()
for emailx in ALL_Emails:
    idnoxx = get_user_idnos(emailx)
    for idnox in idnoxx:
        idnox = int(idnox)
        print(f">>{idnox}<<")
        print(emailx)
        User_List_announce_first = get_dataload_to_announce_mtfirst(emailx, idnox)
        print(len(User_List_announce_first))
        print(User_List_announce_first)
        print(f"{'_'*60}")
"""

def get_dataload_to_announce_mtpublished(email, idnox, daysdelta = 1, weeksdelta = 20):
    #linked to get_user_interest
    #linked to user_announcedDB
    #Gets the last X ocids which have not been announced and have the award_len<7
    user_idnos = get_user_idnos(email)
    User_List_announce_first= []
    dfx = pd.DataFrame(columns=configs.Users_announced_Cols)
    query = f"""
            SELECT user_email, announce_tip, ocid
            FROM public.users_announced prc
            WHERE user_email = '{email}'
            AND announce_tip = '{configs.announce_tip_mtpublished}'
            ORDER BY id ASC 
            """
    dfx = DB_Scripts.sql_readpd_custom(configs.Users_announced_Table , query)
    ocids = dfx["ocid"].tolist()
    #get_user_latest_ocids_idno(email, idnox,awarded_bool, statut ="active", limit=2, daysdelta = 1)
    user_ocids = get_user_latest_ocids_idno(email, idnox, awarded_bool = False, statut= "active, planning", limit=5,  daysdelta=1, weeksdelta=20)
    #~ means not in
    user_ocids = user_ocids[~user_ocids["ocid"].isin(ocids)]
    return user_ocids


"""
ALL_Emails = get_user_allemails()
for emailx in ALL_Emails:
    #if ALL_Emails.index(emailx) == 1:
    print(emailx)
    idnoxx = get_user_idnos(emailx)
    for idnox in idnoxx:
        user_ocids_lx = get_dataload_to_announce_mtpublished(emailx, idnox)
        print(len(user_ocids_lx))
        print(user_ocids_lx["name"].tolist())
        print(user_ocids_lx["idno"].tolist())
"""


def get_dataload_to_announce_mtawarded(email, idnox, daysdelta = 1, weeksdelta = 20):
    #TEXT: Ultima etapă sau a desemnat companiile câștigătoare a procedurii de achiziție.
    #linked to get_user_interest
    #linked to user_announcedDB
    #Gets the last X ocids which have not been announced and have the award_len<7
    user_idnos = get_user_idnos(email)
    User_List_announce_first= []
    dfx = pd.DataFrame(columns = configs.Users_announced_Cols)
    query = f"""
            SELECT user_email, announce_tip, ocid
            FROM public.users_announced prc
            WHERE user_email = '{email}'
            AND announce_tip = '{configs.announce_tip_mtawarded}'
            ORDER BY id ASC 
            """
    dfx = DB_Scripts.sql_readpd_custom(configs.Users_announced_Table , query)
    
    ocids = dfx["ocid"].tolist()
    #get_user_latest_ocids_idno(email, idnox,awarded_bool, statut ="active", limit=2, daysdelta = 1)
    user_ocids = get_user_latest_ocids_idno(email, idnox, awarded_bool = True, statut= "active, planning", limit=5,  daysdelta=5, weeksdelta=5)
    #df[~df.country.isin(countries_to_keep)]
    user_ocids = user_ocids[~user_ocids["ocid"].isin(ocids)]
    return user_ocids

"""
ALL_Emails = get_user_allemails()
for emailx in ALL_Emails:
    idnoxx = get_user_idnos(emailx)
    for idnox in idnoxx:
        idnox = int(idnox)
        print(f">>{idnox}<<")
        print(emailx)
        User_List_announce_awd = get_dataload_to_announce_mtawarded(emailx, idnox)
        User_List_announce_awdx = get_dataload_to_announce_mtawarded("vicunegru@yahoo.com", 1006601001012)
        print(len(User_List_announce_awd))
        print(User_List_announce_awd["name"].tolist())
        print(User_List_announce_awd["idno"].tolist())
        try:
            print(User_List_announce_awd["ocid"].tolist()[0])
        except:
            pass
        print(f"{'_'*60}")
"""