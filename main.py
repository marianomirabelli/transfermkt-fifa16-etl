import mysql
import requests
import random
import pandas as pd
from sqlalchemy import create_engine

import rfs_db
import transfer_mkt as transfer_mkt_impl
import pymysql

import fifa_db as fifa_db_client


def player_name_df():
    headers = {
        'name': [],
        'commentaryid': [],
        'nameId': [],
    }
    return pd.DataFrame(headers, dtype=object)


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

    engine = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")


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

    output_directory = r'C:\Users\54116\Documents\Juegos\FIFA 16\My-Mods\2nd-Uruguay\FIFA-DB'
    players_file=output_directory+r'\players.txt'
    players_team_file = output_directory+r'\teamplayerlinks.txt'
    player_names_file = output_directory+r'\playernames.txt'
    dc_names_file = output_directory+r'\dcplayernames.txt'

    player_data_frame.to_sql('players', engine, if_exists='append', index=False)
    player_name_data_frame.to_sql('playernames', engine, if_exists='append', index=False)
    player_name_dlc_data_frame.to_sql('dcplayernames', engine, if_exists='append', index=False)
    player_team_data_frame.to_sql('teamplayerlinks', engine, if_exists='append', index=False)

    player_data_frame.to_csv(players_file, sep='\t', index=False, header=False, mode='a')
    player_name_data_frame.to_csv(player_names_file, sep='\t', index=False, header=False, mode='a')
    player_name_dlc_data_frame.to_csv(dc_names_file, sep='\t', index=False, header=False, mode='a')
    player_team_data_frame.to_csv(players_team_file, sep='\t', index=False, header=False, mode='a')



# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    fifa_team_id = 130020
    team_import_id = 115535

    rfs_db.drop_players_rfs_table()
    rfs_db.drop_team_player_links_rfs_table()

    rfs_db.load_players_rfs_from_file()
    rfs_db.load_players_team_links_rfs_from_file()

    create_players_from_club(team_import_id, fifa_team_id)

# FC Taraba matches with Plateau United. Test with Katsina United if possible
# Gombe United matches Sharks FC
