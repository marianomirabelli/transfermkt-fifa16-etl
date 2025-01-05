from sqlalchemy import create_engine, text
import pandas as pd

engine = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")

def get_max_name_id():
    query = text("""
        SELECT MAX(nameid) AS max_name_id
        FROM `rfs-fifa`.`playernames`
    """)
    max_dc_name_id_df = pd.read_sql(query, engine)

    max_dc_name_id = max_dc_name_id_df.iloc[0]['max_name_id']

    return max_dc_name_id

def get_max_player_id():
    query = text("""
        SELECT MAX(playerId) AS max_player_id
        FROM `rfs-fifa`.`players`
    """)
    max_player_id_df = pd.read_sql(query, engine)

    max_player_id = max_player_id_df.iloc[0]['max_player_id']

    return max_player_id

def get_max_artificial_id():
    query = text("""
        SELECT MAX(artificialkey) AS max_artificial_id
        FROM `rfs-fifa`.`teamplayerlinks`
    """)
    max_artificial_id_df = pd.read_sql(query, engine)

    max_artificial_id = max_artificial_id_df.iloc[0]['max_artificial_id']

    return max_artificial_id


def fill_player_names_df(df, player_name, player_last_name,comment_id):
    max_name_id = get_max_dc_name_id()["max_nameid"]
    name_row = {
        'name': player_name,
        'commentaryid': comment_id,
        'nameId': max_name_id+1
    }

    last_name_row = {
        'name': player_last_name,
        'commentaryid': comment_id,
        'nameId': max_name_id+2
    }
    df = pd.concat([df, pd.DataFrame([name_row],dtype=object)], ignore_index=True)
    df = pd.concat([df, pd.DataFrame([last_name_row],dtype=object)], ignore_index=True)
    return df
