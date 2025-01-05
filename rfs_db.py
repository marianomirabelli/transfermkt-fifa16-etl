import pandas as pd
import transfer_mkt
import random
from sqlalchemy import create_engine, text

# Create the database engine (replace with your credentials)
engine = create_engine("mysql+pymysql://root:root@localhost:3306/rfs-fifa")


# Example: Reading a full table into a DataFrame
def query_players_by_team_id(team_id):
        query = text("""
                SELECT * 
                FROM `rfs-fifa`.`players-rfs` 
                WHERE playerid IN (
                    SELECT playerid 
                    FROM `rfs-fifa`.`teamplayerlinks-rfs` 
                    WHERE teamid = :team_id
                )
            """)
        # Use parameterized query to safely pass the `team_id`
        players_df = pd.read_sql(query, engine, params={"team_id": team_id})
        return players_df

def query_team_by_id(team_id):
    query = text("""
        SELECT * 
        FROM `rfs-fifa`.`teams-rfs` 
        WHERE teamid = :team_id
    """)
    # Use parameterized query to safely pass the `team_id`
    team_df = pd.read_sql(query, engine, params={"team_id": team_id})
    return team_df

def fill_player_dataframe(df, rfs_player_df,player_id, first_name_id, last_name_id):
    shoetypecode = random.randint(0, 255)
    haircolorcode = rfs_player_df["haircolor"]
    facialhairtypecode = rfs_player_df["facialhairtype"]
    curve = rfs_player_df["curve"]
    jerseystylecode = 0  # Normal
    agility = rfs_player_df["agility"]
    accessorycode4 = rfs_player_df["accessorytype4"]
    gksavetype = rfs_player_df["gksavetype"]
    positioning = rfs_player_df["positioning"]
    hairtypecode = rfs_player_df["hairtype"]
    standingtackle = rfs_player_df["standingtackle"]
    faceposercode = 0
    preferredposition3 = rfs_player_df["preferredposition3"]
    longpassing = rfs_player_df["longpassing"]
    penalties = rfs_player_df["penalties"]
    animfreekickstartposcode = random.randint(0, 1)
    animpenaltieskickstylecode = random.randint(0, 2)
    isretiring = 0
    longshots = rfs_player_df["longshots"]
    gkdiving = rfs_player_df["gkdiving"]
    interceptions = rfs_player_df["interceptions"]
    shoecolorcode2 = random.randint(0, 31)
    crossing = rfs_player_df["crossing"]
    potential = rfs_player_df["potential"]
    gkreflexes = rfs_player_df["gkreflexes"]
    finishingcode1 = 0
    reactions = rfs_player_df["reactions"]
    vision = rfs_player_df["vision"]
    contractvaliduntil = random.randint(2026, 2027)
    finishing = rfs_player_df["finishing"]
    dribbling = rfs_player_df["dribbling"]
    slidingtackle = rfs_player_df["slidingtackle"]
    accessorycode3 = 0
    accessorycolourcode1 = random.randint(0, 1)
    headtypecode = rfs_player_df["headtype"]
    firstnameid = first_name_id
    sprintspeed = rfs_player_df["sprintspeed"]
    height = rfs_player_df["height"]
    hasseasonaljersey = random.randint(0, 4)
    preferredposition2 = rfs_player_df["preferredposition2"]
    strength = rfs_player_df["strength"]
    birthdate = random.randint(148368,154211) ##rfs_player_df["birthdate"]  ## TODO: Be careful with this. Likely RFS format is not FIFA format
    preferredposition1 = rfs_player_df["preferredposition1"]
    ballcontrol = rfs_player_df["ballcontrol"]
    shotpower = rfs_player_df["shotpower"]
    trait1 = rfs_player_df["trait"]
    socklengthcode = rfs_player_df["socklength"]
    weight = rfs_player_df["weight"]
    hashighqualityhead = rfs_player_df["hashighqualityhead"]
    gkglovetypecode = random.randint(0, 127)
    balance = rfs_player_df["balance"]
    gender = 0
    gkkicking = rfs_player_df["gkkicking"]
    internationalrep = rfs_player_df["internationalrep"]
    animpenaltiesmotionstylecode = random.randint(0, 1)
    shortpassing = rfs_player_df["shortpassing"]
    freekickaccuracy = rfs_player_df["freekickaccuracy"]
    skillmoves = rfs_player_df["skillmoves"]
    usercaneditname = 0
    attackingworkrate = rfs_player_df["attackingworkrate"]
    finishingcode2 = rfs_player_df["finishingstyle2"]
    aggression = rfs_player_df["aggression"]
    acceleration = rfs_player_df["acceleration"]
    headingaccuracy = rfs_player_df["headingaccuracy"]
    eyebrowcode = rfs_player_df["eyebrow"]
    runningcode2 = rfs_player_df["runningstyle2"]
    gkhandling = rfs_player_df["gkhandling"]
    eyecolorcode = rfs_player_df["eyecolor"]
    jerseysleevelengthcode = 0
    accessorycolourcode3 = rfs_player_df["accessorycolour3"]
    accessorycode1 = rfs_player_df["accessorytype1"]
    playerjointeamdate = transfer_mkt.parse_joined_on()  ## TODO: Fix date format
    headclasscode = 1  ## Not special head
    defensiveworkrate = rfs_player_df["defensiveworkrate"]
    nationality = rfs_player_df["nationality"]
    preferredfoot = rfs_player_df["preferredfoot"]
    sideburnscode = 0
    weakfootabilitytypecode = rfs_player_df["weakfootability"]
    jumping = rfs_player_df["jumping"]
    skintypecode = rfs_player_df["skintype"]
    gkkickstyle = rfs_player_df["gkkickstyle"]
    stamina = rfs_player_df["stamina"]
    playerid = player_id
    marking = rfs_player_df["marking"]
    accessorycolourcode4 = rfs_player_df["accessorycolour4"]
    gkpositioning = rfs_player_df["gkpositioning"]
    trait2 = 0
    skintonecode = rfs_player_df["skintone"]  # Give a random between african and arabic styles
    shortstyle = 0
    overallrating = rfs_player_df["overallrating"]
    emotion = rfs_player_df["emotion"]
    jerseyfit = 0
    accessorycode2 = rfs_player_df["accessorytype2"]
    shoedesigncode = 0
    playerjerseynameid = last_name_id  ## TODO: check if this id comes from players_names file
    shoecolorcode1 = 30
    commonnameid = 0
    bodytypecode = rfs_player_df["bodytype"]
    animpenaltiesstartposcode = random.randint(0, 2)
    runningcode1 = rfs_player_df["runningstyle1"]
    preferredposition4 = rfs_player_df["preferredposition4"]
    volleys = rfs_player_df["volleys"]
    accessorycolourcode2 = rfs_player_df["accessorycolour2"]
    facialhaircolorcode = rfs_player_df["facialhaircolor"]  # Negro

    new_player_row = {
            'shoetypecode': shoetypecode,
            'haircolorcode': haircolorcode,
            'facialhairtypecode': facialhairtypecode,
            'curve': curve,
            'jerseystylecode': jerseystylecode,
            'agility': agility,
            'accessorycode4': accessorycode4,
            'gksavetype': gksavetype,
            'positioning': positioning,
            'hairtypecode': hairtypecode,
            'standingtackle': standingtackle,
            'faceposercode': faceposercode,
            'preferredposition3': preferredposition3,
            'longpassing': longpassing,
            'penalties': penalties,
            'animfreekickstartposcode': animfreekickstartposcode,
            'animpenaltieskickstylecode': animpenaltieskickstylecode,
            'isretiring': isretiring,
            'longshots': longshots,
            'gkdiving': gkdiving,
            'interceptions': interceptions,
            'shoecolorcode2': shoecolorcode2,
            'crossing': crossing,
            'potential': potential,
            'gkreflexes': gkreflexes,
            'finishingcode1': finishingcode1,
            'reactions': reactions,
            'vision': vision,
            'contractvaliduntil': contractvaliduntil,
            'finishing': finishing,
            'dribbling': dribbling,
            'slidingtackle': slidingtackle,
            'accessorycode3': accessorycode3,
            'accessorycolourcode1': accessorycolourcode1,
            'headtypecode': headtypecode,
            'firstnameid': first_name_id,
            'sprintspeed': sprintspeed,
            'height': height,
            'hasseasonaljersey': hasseasonaljersey,
            'preferredposition2': preferredposition2,
            'strength': strength,
            'birthdate': birthdate,
            'preferredposition1': preferredposition1,
            'ballcontrol': ballcontrol,
            'shotpower': shotpower,
            'trait1': trait1,
            'socklengthcode': socklengthcode,
            'weight': weight,
            'hashighqualityhead': hashighqualityhead,
            'gkglovetypecode': gkglovetypecode,
            'balance': balance,
            'gender': gender,
            'gkkicking': gkkicking,
            'lastnameid': last_name_id,
            'internationalrep': internationalrep,
            'animpenaltiesmotionstylecode': animpenaltiesmotionstylecode,
            'shortpassing': shortpassing,
            'freekickaccuracy': freekickaccuracy,
            'skillmoves': skillmoves,
            'usercaneditname': usercaneditname,
            'attackingworkrate': attackingworkrate,
            'finishingcode2': finishingcode2,
            'aggression': aggression,
            'acceleration': acceleration,
            'headingaccuracy': headingaccuracy,
            'eyebrowcode': eyebrowcode,
            'runningcode2': runningcode2,
            'gkhandling': gkhandling,
            'eyecolorcode': eyecolorcode,
            'jerseysleevelengthcode': jerseysleevelengthcode,
            'accessorycolourcode3': accessorycolourcode3,
            'accessorycode1': accessorycode1,
            'playerjointeamdate': playerjointeamdate,
            'headclasscode': headclasscode,
            'defensiveworkrate': defensiveworkrate,
            'nationality': nationality,
            'preferredfoot': preferredfoot,
            'sideburnscode': sideburnscode,
            'weakfootabilitytypecode': weakfootabilitytypecode,
            'jumping': jumping,
            'skintypecode': skintypecode,
            'gkkickstyle': gkkickstyle,
            'stamina': stamina,
            'playerid': playerid,
            'marking': marking,
            'accessorycolourcode4': accessorycolourcode4,
            'gkpositioning': gkpositioning,
            'trait2': trait2,
            'skintonecode': skintonecode,
            'shortstyle': shortstyle,
            'overallrating': overallrating,
            'emotion': emotion,
            'jerseyfit': jerseyfit,
            'accessorycode2': accessorycode2,
            'shoedesigncode': shoedesigncode,
            'playerjerseynameid': playerjerseynameid,
            'shoecolorcode1': shoecolorcode1,
            'commonnameid': commonnameid,
            'bodytypecode': bodytypecode,
            'animpenaltiesstartposcode': animpenaltiesstartposcode,
            'runningcode1': runningcode1,
            'preferredposition4': preferredposition4,
            'volleys': volleys,
            'accessorycolourcode2': accessorycolourcode2,
            'facialhaircolorcode': facialhaircolorcode
        }

    df = pd.concat([df, pd.DataFrame([new_player_row], dtype=object)], ignore_index=True)
    return df