import os

import mysql
import requests
import random
import pandas as pd
from sqlalchemy import create_engine

import fifa_db
import rfs_db
import transfer_mkt as transfer_mkt_impl
import pymysql

import fifa_db as fifa_db_client

MOD_FIFA_DB_BASE_PATH = r'C:\Users\54116\Documents\Juegos\FIFA 16\My-Mods\2nd-Uruguay\FIFA-DB'
DB_ENGINE = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")

default_team_map = {
    'assetid': 130020,
    'balltype': 113,
    'teamcolor1g': 255,
    'teamcolor1r': 255,
    'teamcolor2b': 0,
    'teamcolor2r': 0,
    'teamcolor3r': 128,
    'teamcolor1b': 255,
    'latitude': 48,
    'teamcolor3g': 128,
    'teamcolor2g': 0,
    'teamname': "Test",
    'adboardid': 1,
    'teamcolor3b': 128,
    'defmentality': 50,
    'powid': -1,
    'rightfreekicktakerid': 1,
    'physioid_secondary': 2,
    'domesticprestige': random.randint(1, 10),
    'genericint2': -1,
    'jerseytype': 1,
    'rivalteam': 111235,
    'midfieldrating': 50,
    'matchdayoverallrating': 50,
    'matchdaymidfieldrating': 50,
    'attackrating': 50,
    'physioid_primary': 1,
    'longitude': 10,
    'buspassing': 50,
    'matchdaydefenserating': 50,
    'defenserating': 50,
    'defteamwidth': 50,
    'longkicktakerid': 1,
    'bodytypeid': 1,
    'trait1': 0,
    'busdribbling': 50,
    'rightcornerkicktakerid': 1,
    'suitvariationid': 0,
    'defaggression': 50,
    'ethnicity': 2,
    'leftcornerkicktakerid': 1,
    'teamid': 130020,
    'fancrowdhairskintexturecode': 0,
    'suittypeid': 0,
    'numtransfersin': 0,
    'captainid': 1,
    'personalityid': 0,
    'leftfreekicktakerid': 1,
    'genericbanner': 0,
    'buspositioning': 0,
    'stafftracksuitcolorcode': 0,
    'ccpositioning': 50,
    'busbuildupspeed': 2000000,
    'transferbudget': random.randint(1800000, 3500000),
    'ccshooting': 50,
    'overallrating': 50,
    'ccpassing': 1,
    'utcoffset': 1,
    'penaltytakerid': 1,
    'freekicktakerid': 0,
    'defdefenderline': 10,
    'internationalprestige': 0,
    'form': -1,
    'genericint1': 50,
    'cccrossing': 50,
    'matchdayattackrating': 50
}


def player_name_df():
    headers = {
        'name': [],
        'commentaryid': [],
        'nameId': [],
    }
    return pd.DataFrame(headers, dtype=object)

def team_df():
    return pd.DataFrame(columns=default_team_map.keys())

def player_name_dlc_df():
    headers = {
        'name': [],
        'nameId': [],
    }
    return pd.DataFrame(headers, dtype=object)


def player_team_df():
    headers = {
        'leaguegoals': [],
        'isamongtopscorers': [],
        'yellows': [],
        'isamongtopscorersinteam': [],
        'jerseynumber': [],
        'position': [],
        'artificialkey': [],
        'teamid': [],
        'leaguegoalsprevmatch': [],
        'injury': [],
        'leagueappearances': [],
        'prevform': [],
        'istopscorer': [],
        'leaguegoalsprevthreematches': [],
        'playerid': [],
        'form': [],
        'reds': []
    }
    return pd.DataFrame(headers, dtype=object)


def player_init_df():
    headers = {
        'shoetypecode': [],
        'haircolorcode': [],
        'facialhairtypecode': [],
        'curve': [],
        'jerseystylecode': [],
        'agility': [],
        'accessorycode4': [],
        'gksavetype': [],
        'positioning': [],
        'hairtypecode': [],
        'standingtackle': [],
        'faceposercode': [],
        'preferredposition3': [],
        'longpassing': [],
        'penalties': [],
        'animfreekickstartposcode': [],
        'animpenaltieskickstylecode': [],
        'isretiring': [],
        'longshots': [],
        'gkdiving': [],
        'interceptions': [],
        'shoecolorcode2': [],
        'crossing': [],
        'potential': [],
        'gkreflexes': [],
        'finishingcode1': [],
        'reactions': [],
        'vision': [],
        'contractvaliduntil': [],
        'finishing': [],
        'dribbling': [],
        'slidingtackle': [],
        'accessorycode3': [],
        'accessorycolourcode1': [],
        'headtypecode': [],
        'firstnameid': [],
        'sprintspeed': [],
        'height': [],
        'hasseasonaljersey': [],
        'preferredposition2': [],
        'strength': [],
        'birthdate': [],
        'preferredposition1': [],
        'ballcontrol': [],
        'shotpower': [],
        'trait1': [],
        'socklengthcode': [],
        'weight': [],
        'hashighqualityhead': [],
        'gkglovetypecode': [],
        'balance': [],
        'gender': [],
        'gkkicking': [],
        'lastnameid': [],
        'internationalrep': [],
        'animpenaltiesmotionstylecode': [],
        'shortpassing': [],
        'freekickaccuracy': [],
        'skillmoves': [],
        'usercaneditname': [],
        'attackingworkrate': [],
        'finishingcode2': [],
        'aggression': [],
        'acceleration': [],
        'headingaccuracy': [],
        'eyebrowcode': [],
        'runningcode2': [],
        'gkhandling': [],
        'eyecolorcode': [],
        'jerseysleevelengthcode': [],
        'accessorycolourcode3': [],
        'accessorycode1': [],
        'playerjointeamdate': [],
        'headclasscode': [],
        'defensiveworkrate': [],
        'nationality': [],
        'preferredfoot': [],
        'sideburnscode': [],
        'weakfootabilitytypecode': [],
        'jumping': [],
        'skintypecode': [],
        'gkkickstyle': [],
        'stamina': [],
        'playerid': [],
        'marking': [],
        'accessorycolourcode4': [],
        'gkpositioning': [],
        'trait2': [],
        'skintonecode': [],
        'shortstyle': [],
        'overallrating': [],
        'emotion': [],
        'jerseyfit': [],
        'accessorycode2': [],
        'shoedesigncode': [],
        'playerjerseynameid': [],
        'shoecolorcode1': [],
        'commonnameid': [],
        'bodytypecode': [],
        'animpenaltiesstartposcode': [],
        'runningcode1': [],
        'preferredposition4': [],
        'volleys': [],
        'accessorycolourcode2': [],
        'facialhaircolorcode': []
    }

    return pd.DataFrame(headers, dtype=object)


def fill_player_names_df(df, player_name, player_last_name, player_name_id, player_lastname_id, comment_id):
    name_row = {
        'name': player_name,
        'commentaryid': comment_id,
        'nameId': player_name_id
    }

    last_name_row = {
        'name': player_last_name,
        'commentaryid': comment_id,
        'nameId': player_lastname_id
    }
    df = pd.concat([df, pd.DataFrame([name_row], dtype=object)], ignore_index=True)
    df = pd.concat([df, pd.DataFrame([last_name_row], dtype=object)], ignore_index=True)
    return df


def fill_player_dlc_names_df(df, player_name, player_last_name, player_name_id, player_lastname_id):
    name_row = {
        'name': player_name,
        'nameId': player_name_id
    }

    last_name_row = {
        'name': player_last_name,
        'nameId': player_lastname_id
    }
    df = pd.concat([df, pd.DataFrame([name_row], dtype=object)], ignore_index=True)
    df = pd.concat([df, pd.DataFrame([last_name_row], dtype=object)], ignore_index=True)
    return df



def fill_players_team_link(df, fifa_team_id, player_id, player_position, artificial_id):
    leaguegoals = 0
    isamongtopscorers = 0
    yellows = 0
    isamongtopscorersinteam = 0
    jerseynumber = random.randint(1, 50)
    position = player_position
    artificialkey = artificial_id
    teamid = fifa_team_id
    leaguegoalsprevmatch = 0
    injury = 0
    leagueappearances = 0
    prevform = 3
    istopscorer = 0
    leaguegoalsprevthreematches = 0
    playerid = player_id
    form = random.randint(1, 3)
    reds = 0

    new_row = {
        'leaguegoals': leaguegoals,
        'isamongtopscorers': isamongtopscorers,
        'yellows': yellows,
        'isamongtopscorersinteam': isamongtopscorersinteam,
        'jerseynumber': jerseynumber,
        'position': position,
        'artificialkey': artificialkey,
        'teamid': teamid,
        'leaguegoalsprevmatch': leaguegoalsprevmatch,
        'injury': injury,
        'leagueappearances': leagueappearances,
        'prevform': prevform,
        'istopscorer': istopscorer,
        'leaguegoalsprevthreematches': leaguegoalsprevthreematches,
        'playerid': playerid,
        'form': form,
        'reds': reds
    }

    df = pd.concat([df, pd.DataFrame([new_row], dtype=object)], ignore_index=True)
    return df

def create_club_from_rfs(rfs_club_id):
    df = team_df()
    rfs_team_df = rfs_db.query_team_by_id(rfs_club_id)
    fifa_team_id = fifa_db_client.get_max_team_id() + 1
    fifa_team_asset_id = fifa_db_client.get_max_asset_id() + 1

    override_fields = [
        'domesticprestige', 'teamcolor1b', 'teamcolor1g', 'teamcolor1r',
        'teamcolor2b', 'teamcolor2g', 'teamcolor2r', 'teamcolor3b', 'teamcolor3g', 'teamcolor3r'
    ]

    new_row = {
        key: (rfs_team_df[key][0] if key in override_fields else default_team_map[key])
        for key in default_team_map
    }

    new_row['teamid'] = fifa_team_id
    new_row['assetid'] = fifa_team_asset_id
    new_row['teamname'] = rfs_team_df['name'][0]

    # Assuming df already exists, otherwise initialize it as an empty DataFrame first
    df = pd.concat([df, pd.DataFrame([new_row], dtype=object)], ignore_index=True)

    teams_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'teams.txt')
    df.to_sql('teams', DB_ENGINE, if_exists='append', index=False)
    df.to_csv(teams_file, sep='\t', index=False, header=False, mode='a', encoding='utf-16')

    return fifa_team_id

def create_players_from_club(team_import_id, fifa_team_id):
    players_list = rfs_db.query_players_by_team_id(team_import_id)

    first_name_id = fifa_db_client.get_max_name_id() + 1
    last_name_id = first_name_id + 1
    player_id = fifa_db_client.get_max_player_id() + 1
    artificial_id = fifa_db_client.get_max_artificial_id() + 1

    player_data_frame = player_init_df()
    player_name_data_frame = player_name_df()
    player_name_dlc_data_frame = player_name_dlc_df()
    player_team_data_frame = player_team_df()



    for index, row in players_list.iterrows():
        first_name = row['firstname']
        last_name = row['lastname']
        player_data_frame = rfs_db.fill_player_dataframe(player_data_frame, row, player_id, first_name_id, last_name_id)
        player_name_data_frame = fill_player_names_df(player_name_data_frame, first_name, last_name, first_name_id,
                                                      last_name_id, 900000)
        player_name_dlc_data_frame = fill_player_dlc_names_df(player_name_dlc_data_frame, first_name, last_name,
                                                              first_name_id, last_name_id)
        player_team_data_frame = fill_players_team_link(player_team_data_frame, fifa_team_id, player_id,
                                                        row['preferredposition1'], artificial_id)
        artificial_id += 1
        first_name_id = last_name_id + 1
        last_name_id = first_name_id + 1
        player_id += 1

    # mydb = mysql.connector.connect(
    #     host="localhost",
    #     user="root",
    #     database="fifa-db"
    # )

    ##Delete current players from team
    # print("Connection established")
    #cursor = mydb.cursor()
    #cursor.execute("DELETE FROM fifa16.teamplayerlinks WHERE teamid=%s"%fifa_club_id)
    #mydb.commit()

    players_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'players.txt')
    players_team_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'teamplayerlinks.txt')
    player_names_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'playernames.txt')
    dc_names_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'dcplayernames.txt')

    player_data_frame.to_sql('players', DB_ENGINE, if_exists='append', index=False)
    player_name_data_frame.to_sql('playernames', DB_ENGINE, if_exists='append', index=False)
    player_name_dlc_data_frame.to_sql('dcplayernames', DB_ENGINE, if_exists='append', index=False)
    player_team_data_frame.to_sql('teamplayerlinks', DB_ENGINE, if_exists='append', index=False)

    player_data_frame.to_csv(players_file, sep='\t', index=False, header=False, mode='a', encoding='utf-16')
    player_name_data_frame.to_csv(player_names_file, sep='\t', index=False, header=False, mode='a',encoding='utf-16')
    player_name_dlc_data_frame.to_csv(dc_names_file, sep='\t', index=False, header=False, mode='a',encoding='utf-16')
    player_team_data_frame.to_csv(players_team_file, sep='\t', index=False, header=False, mode='a',encoding='utf-16')

def init_import_context():
    rfs_db.drop_teams_rfs_table()
    rfs_db.drop_players_rfs_table()
    rfs_db.drop_team_player_links_rfs_table()

    rfs_db.load_team_from_file()
    rfs_db.load_players_rfs_from_file()
    rfs_db.load_players_team_links_rfs_from_file()

    fifa_db.drop_table("teams")
    fifa_db.drop_table("players")
    fifa_db.drop_table("playernames")
    fifa_db.drop_table("teamplayerlinks")
    fifa_db.drop_table("dcplayernames")

    players_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'players.txt')
    players_team_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'teamplayerlinks.txt')
    player_names_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'playernames.txt')
    dc_names_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'dcplayernames.txt')
    teams_file = os.path.join(MOD_FIFA_DB_BASE_PATH, 'teams.txt')

    fifa_db.load_table_from_file(teams_file,'teams')
    fifa_db.load_table_from_file(players_file,'players')
    fifa_db.load_table_from_file(players_team_file,'teamplayerlinks')
    fifa_db.load_table_from_file(player_names_file,'playernames')
    fifa_db.load_table_from_file(dc_names_file, 'dcplayernames')

def run_import(team_import_id):
    fifa_created_team_id = create_club_from_rfs(team_import_id)
    create_players_from_club(team_import_id, fifa_created_team_id)

def del_players_from_team(fifa_team_id):
    fifa_db.delete_players_names(fifa_team_id)
    fifa_db.delete_players_and_team_players_link(fifa_team_id)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    team_import_id = 130372
    #del_players_from_team(fifa_team_id)
    init_import_context()
    run_import(team_import_id)


# FC Taraba matches with Plateau United. Test with Katsina United if possible
# Gombe United matches Sharks FC
