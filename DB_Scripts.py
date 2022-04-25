import configs
from pandas import read_sql



def insert_df_data(dfx, table, engine = configs.engine, if_exists="append"):
    with engine.connect() as conn:
        dfx.to_sql(f'{table}', con=conn, if_exists = if_exists, index=False)


def execute_sql(sql_text , engine = configs.engine):
    with engine.connect() as connection:
        result = connection.execute((sql_text))


def sql_readpd(table, engine = configs.engine):
    query =f"""SELECT *
                        FROM public.{table}
                        """
    with engine.connect() as conn:
        dfx = read_sql(query, con=conn)
    return dfx

def sql_readpd_custom(table,query,  engine = configs.engine):
    with engine.connect() as conn:
        dfx = read_sql(query, con=conn)
    return dfx