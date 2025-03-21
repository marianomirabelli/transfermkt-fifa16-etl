from sqlalchemy import create_engine, text
import pandas as pd
import chardet


engine = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")

def delete_players_names(team_id):
    query = text("""
        SELECT firstnameid, lastnameid FROM `rfs-fifa`.`players`
        WHERE playerid IN (
            SELECT playerid
            FROM `rfs-fifa`.`teamplayerlinks`
            WHERE teamid = :team_id
        )
    """)
    name_tuple = pd.read_sql(query, engine, params={"team_id": team_id})

    # Iterate through the result set
    with engine.connect() as connection:
        for _, row in name_tuple.iterrows():
            firstnameid = row['firstnameid']
            lastnameid = row['lastnameid']

            # Delete from playernames table
            delete_playernames_query = text("""
                DELETE FROM `rfs-fifa`.`playernames`
                WHERE nameId = :name_id
            """)
            connection.execute(delete_playernames_query, {"name_id": firstnameid})
            connection.execute(delete_playernames_query, {"name_id": lastnameid})

            # Delete from dcplayernames table
            delete_dcplayernames_query = text("""
                DELETE FROM `rfs-fifa`.`dcplayernames`
                WHERE nameId = :name_id
            """)
            connection.execute(delete_dcplayernames_query, {"name_id": firstnameid})
            connection.execute(delete_dcplayernames_query, {"name_id": lastnameid})

def delete_players_and_team_players_link(team_id):
    del_players = text("""
        DELETE FROM `rfs-fifa`.`players`
        WHERE playerid IN (
            SELECT playerid
            FROM `rfs-fifa`.`teamplayerlinks`
            WHERE teamid = :team_id
        )
    """)
    del_links = text("""
        DELETE FROM `rfs-fifa`.`teamplayerlinks`
        WHERE teamid = :team_id
    """)
    with engine.connect() as connection:
        connection.execute(del_players, {"team_id": team_id})
        connection.execute(del_links, {"team_id": team_id})


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

def get_max_team_id():
    query = text("""
        SELECT MAX(teamId) AS max_team_id
        FROM `rfs-fifa`.`teams`
    """)
    max_team_id_df = pd.read_sql(query, engine)

    max_player_id = max_team_id_df.iloc[0]['max_team_id']

    return max_player_id

def get_max_asset_id():
    query = text("""
        SELECT MAX(assetid) AS max_asset_id
        FROM `rfs-fifa`.`teams`
    """)
    max_asset_id_df = pd.read_sql(query, engine)

    max_asset_id = max_asset_id_df.iloc[0]['max_asset_id']

    return max_asset_id

def get_max_artificial_id():
    query = text("""
        SELECT MAX(artificialkey) AS max_artificial_id
        FROM `rfs-fifa`.`teamplayerlinks`
    """)
    max_artificial_id_df = pd.read_sql(query, engine)

    max_artificial_id = max_artificial_id_df.iloc[0]['max_artificial_id']

    return max_artificial_id
