from sqlalchemy import create_engine, text
import pandas as pd
import chardet


engine = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")

def detect_encoding(file_path):
    with open(file_path, 'rb') as file:
        rawdata = file.read(10000)  # Read a chunk of the file
        result = chardet.detect(rawdata)
        return result['encoding']

def drop_table(table_name):
    with engine.connect() as connection:
        drop_query = f"DROP TABLE IF EXISTS {table_name};"
        connection.execute(text(drop_query))

def load_table_from_file(file_path, table_name):
    detected_encoding = detect_encoding(file_path)
    df = pd.read_csv(file_path, sep="\t", encoding=detected_encoding)
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

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
