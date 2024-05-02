from datetime import datetime

import requests
import random
import pandas as pd

foot_map = {
    'right': 1,
    'left': 2,
    'both': random.randint(1,2)
}

second_foot_map = {
    'right': lambda: random.randint(1,3),
    'left': lambda: random.randint(1,3),
    'both': lambda: random.randint(4,5)
}

face_style_map= {
    #African
    1: lambda: [random.randint(1000,1027), random.randint(3000,3005), random.randint(4500,4502), random.randint(4525,4525),random.randint(5000,5003)],
    #Asiatican
    2: lambda: [random.randint(500,532)],
    #Caucasico
    3: lambda: [random.randint(0,25), random.randint(2000,2017), random.randint(2019,2030),random.randint(3500,3505), random.randint(4000,4003)],
    #Latin
    4: lambda: [random.randint(1500,1528), random.randint(2500,2518)]
}

preferred_position_map = {
    'none': -1,
    'goalkeeper': 0,
    'centreback': 5,
    'leftback': 7,
    'rightback': 3,
    'defensivemidfield': 10,
    'centralmidfield':14,
    'attackingmidfield':18,
    'rightmidfield':12,
    'leftmidfield':16,
    'leftwinger': 27,
    'rightwinger': 23,
    'secondstriker': 21,
    'centreforward': 25
}

nations_map = {
    'Albania': 1,
    'Andorra': 2,
    'Armenia': 3,
    'Austria': 4,
    'Azerbaijan': 5,
    'Belarus': 6,
    'Belgium': 7,
    'Bosnia-Herzegovina': 8,
    'Bulgaria': 9,
    'Croatia': 10,
    'Cyprus': 11,
    'Czech Republic': 12,
    'Denmark': 13,
    'England': 14,
    'Montenegro': 15,
    'Faroe Islands': 16,
    'Finland': 17,
    'France': 18,
    'North Macedonia': 19,
    'Georgia': 20,
    'Germany': 21,
    'Greece': 22,
    'Hungary': 23,
    'Iceland': 24,
    'Republic of Ireland': 25,
    'Israel': 26,
    'Italy': 27,
    'Latvia': 28,
    'Liechtenstein': 29,
    'Lithuania': 30,
    'Luxembourg': 31,
    'Malta': 32,
    'Moldova': 33,
    'Netherlands': 34,
    'Northern Ireland': 35,
    'Norway': 36,
    'Poland': 37,
    'Portugal': 38,
    'Romania': 39,
    'Russia': 40,
    'San Marino': 41,
    'Scotland': 42,
    'Slovakia': 43,
    'Slovenia': 44,
    'Spain': 45,
    'Sweden': 46,
    'Switzerland': 47,
    'Turkey': 48,
    'Ukraine': 49,
    'Wales': 50,
    'Serbia': 51,
    'Argentina': 52,
    'Bolivia': 53,
    'Brazil': 54,
    'Chile': 55,
    'Colombia': 56,
    'Ecuador': 57,
    'Paraguay': 58,
    'Peru': 59,
    'Uruguay': 60,
    'Venezuela': 61,
    'Anguilla': 62,
    'Antigua and Barbuda': 63,
    'Aruba': 64,
    'Bahamas': 65,
    'Barbados': 66,
    'Belize': 67,
    'Bermuda': 68,
    'British Virgin Islands': 69,
    'Canada': 70,
    'Cayman Islands': 71,
    'Costa Rica': 72,
    'Cuba': 73,
    'Dominica': 74,
    'International Men': 75,
    'El Salvador': 76,
    'Grenada': 77,
    'Guatemala': 78,
    'Guyana': 79,
    'Haiti': 80,
    'Honduras': 81,
    'Jamaica': 82,
    'Mexico': 83,
    'Montserrat': 84,
    'Curaçao': 85,
    'Nicaragua': 86,
    'Panama': 87,
    'Puerto Rico': 88,
    'St. Kitts and Nevis': 89,
    'St. Lucia': 90,
    'St. Vincent and the Grenadines': 91,
    'Suriname': 92,
    'Trinidad and Tobago': 93,
    'Turks and Caicos Islands': 94,
    'United States': 95,
    'US Virgin Islands': 96,
    'Tunisia': 97,
    'Angola': 98,
    'Benin': 99,
    'Botswana': 100,
    'Burkina Faso': 101,
    'Burundi': 102,
    'Cameroon': 103,
    'Cape Verde Islands': 104,
    'Central African Republic': 105,
    'Chad': 106,
    'Congo': 107,
    'Côte d\'Ivoire': 108,
    'Djibouti': 109,
    'Congo DR': 110,
    'Egypt': 111,
    'Equatorial Guinea': 112,
    'Eritrea': 113,
    'Ethiopia': 114,
    'Gabon': 115,
    'Gambia': 116,
    'Ghana': 117,
    'Guinea': 118,
    'Guinea-Bissau': 119,
    'Kenya': 120,
    'Lesotho': 121,
    'Liberia': 122,
    'Libya': 123,
    'Madagascar': 124,
    'Malawi': 125,
    'Mali': 126,
    'Mauritania': 127,
    'Mauritius': 128,
    'Morocco': 129,
    'Mozambique': 130,
    'Namibia': 131,
    'Niger': 132,
    'Nigeria': 133,
    'Rwanda': 134,
    'São Tomé e Príncipe': 135,
    'Senegal': 136,
    'Seychelles': 137,
    'Sierra Leone': 138,
    'Somalia': 139,
    'South Africa': 140,
    'Sudan': 141,
    'Eswatini': 142,
    'Tanzania': 143,
    'Togo': 144,
    'Algeria': 145,
    'Uganda': 146,
    'Zambia': 147,
    'Zimbabwe': 148,
    'Afghanistan': 149,
    'Bahrain': 150,
    'Bangladesh': 151,
    'Bhutan': 152,
    'Brunei Darussalam': 153,
    'Cambodia': 154,
    'China PR': 155,
    'Guam': 157,
    'Hong Kong': 158,
    'India': 159,
    'Indonesia': 160,
    'Iran': 161,
    'Iraq': 162,
    'Japan': 163,
    'Jordan': 164,
    'Kazakhstan': 165,
    'Korea DPR': 166,
    'Korea Republic': 167,
    'Kuwait': 168,
    'Kyrgyzstan': 169,
    'Laos': 170,
    'Lebanon': 171,
    'Macau': 172,
    'Malaysia': 173,
    'Maldives': 174,
    'Mongolia': 175,
    'Myanmar': 176,
    'Nepal': 177,
    'Oman': 178,
    "Pakistan": 179,
    "Palestine": 180,
    "Philippines": 181,
    "Qatar": 182,
    "Saudi Arabia": 183,
    "Singapore": 184,
    "Sri Lanka": 185,
    "Syria": 186,
    "Tajikistan": 187,
    "Thailand": 188,
    "Turkmenistan": 189,
    "United Arab Emirates": 190,
    "Uzbekistan": 191,
    "Vietnam": 192,
    "Yemen": 193,
    "American Samoa": 194,
    "Australia": 195,
    "Cook Islands": 196,
    "Fiji": 197,
    "New Zealand": 198,
    "Papua New Guinea": 199,
    "Samoa": 200,
    "Solomon Islands": 201,
    "Tahiti": 202,
    "Tonga": 203,
    "Vanuatu": 204,
    "Gibraltar": 205,
    "Greenland": 206,
    "Dominican Republic": 207,
    "Estonia": 208,
    "Created Players Country": 209,
    "Free Agents Country": 210,
    "Rest of World": 211,
    "Timor-Leste": 212,
    "Chinese Taipei": 213,
    "Comoros": 214,
    "New Caledonia": 215,
    "Creation Centre Country": 216,
    "South Sudan": 218,
    "Kosovo": 219,
    "International Women": 222
}

def player_name_df():
    headers = {
    'name': [],
    'commentaryid': [],
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

    # Save the dataframe to a CSV file


def fill_player_dataframe(df,player, lastnameid):

        preferred_position = parse_preferred_position(player)

        shoetypecode = random.randint(0, 255)
        haircolorcode = 1  ##Negro
        facialhairtypecode = random.randint(0, 15)
        curve = random.randint(40, 66)
        jerseystylecode = 0  # Normal
        agility = random.randint(50, 68)
        accessorycode4 = 0
        gksavetype = random.randint(0, 1) if preferred_position == 0 else 0
        positioning = random.randint(20, 35) if preferred_position == 0 else random.randint(45, 66)
        hairtypecode = random.randint(0, 124)
        standingtackle = random.randint(18, 25) if preferred_position == 0 else random.randint(45, 66)
        faceposercode = 0
        preferredposition3 = -1
        longpassing = random.randint(18, 25) if preferred_position == 0 else random.randint(45, 70)
        penalties = random.randint(30, 70)
        animfreekickstartposcode = random.randint(0, 1)
        animpenaltieskickstylecode = random.randint(0, 2)
        isretiring = 0
        longshots = random.randint(18, 25) if preferred_position == 0 else random.randint(45, 68)
        gkdiving = random.randint(55, 66) if preferred_position == 0 else random.randint(4, 16)
        interceptions = random.randint(18, 30) if preferred_position == 0 else random.randint(38, 70)
        shoecolorcode2 = random.randint(0, 31)
        crossing = random.randint(18, 30) if preferred_position == 0 else random.randint(40, 66)
        potential = random.randint(40, 73)
        gkreflexes = random.randint(55, 66) if preferred_position == 0 else random.randint(4, 12)
        finishingcode1 = 0
        reactions = random.randint(18, 30) if preferred_position == 0 else random.randint(40, 68)
        vision = random.randint(18, 30) if preferred_position == 0 else random.randint(40, 70)
        contractvaliduntil = random.randint(2025, 2027)
        finishing = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 68)
        dribbling = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        slidingtackle = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        accessorycode3 = 0
        accessorycolourcode1 = random.randint(0, 1)
        headtypecode = face_style_map[random.choice(list(face_style_map.keys()))]()[0]  # Give a random face based on african styles
        sprintspeed = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        height = parse_height(player)
        hasseasonaljersey = random.randint(0, 4)
        preferredposition2 = -1
        strength = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        birthdate = parse_birth_date(player)  ## TODO: Fix date format
        preferredposition1 = parse_preferred_position(player)
        ballcontrol = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        shotpower = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        trait1 = 0
        socklengthcode = random.randint(0, 1)
        weight = random.randint(65, 85)
        hashighqualityhead = 0
        gkglovetypecode = random.randint(0, 127)
        balance = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        gender = 0
        gkkicking = random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)
        internationalrep = random.randint(1, 3)
        animpenaltiesmotionstylecode = random.randint(0, 1)
        shortpassing = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        freekickaccuracy = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        skillmoves = 0 if preferred_position == 0 else random.randint(0, 2)
        usercaneditname = 0
        attackingworkrate = random.randint(0, 2)
        finishingcode2 = 0
        aggression = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        acceleration = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        headingaccuracy = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        eyebrowcode = random.randint(0, 1)
        runningcode2 = random.randint(0, 127)
        gkhandling = random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)
        eyecolorcode = 3
        jerseysleevelengthcode = 0
        accessorycolourcode3 = 0
        accessorycode1 = 0
        playerjointeamdate = parse_joined_on(player)  ## TODO: Fix date format
        headclasscode = 1  ## 1 for Africans
        defensiveworkrate = random.randint(0, 2)
        nationality = parse_nationality(player)
        preferredfoot = parse_foot(player)
        sideburnscode = 0
        weakfootabilitytypecode = parse_second_foot(player)
        jumping = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        skintypecode = random.randint(0, 1)
        gkkickstyle = 0 if preferred_position == 0 else random.randint(0, 3)
        stamina = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        marking = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        accessorycolourcode4 = 0
        gkpositioning = random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)
        trait2 = 0
        skintonecode = random.randint(9, 10)
        shortstyle = 0
        overallrating = random.randint(55, 68)
        emotion = 1
        jerseyfit = 0
        accessorycode2 = 0
        shoedesigncode = 0
        playerjerseynameid = lastnameid  ## TODO: check if this id comes from players_names file
        shoecolorcode1 = 30
        commonnameid = 0
        bodytypecode = random.randint(1, 9)
        animpenaltiesstartposcode = random.randint(0, 2)
        runningcode1 = 0
        preferredposition4 = -1
        volleys = random.randint(18, 25) if preferred_position == 0 else random.randint(44, 70)
        accessorycolourcode2 = 0
        facialhaircolorcode = random.randint(0, 2)

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
            'lastnameid': lastnameid,
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

        df = pd.concat([df, pd.DataFrame([new_player_row],dtype=object)], ignore_index=True)
        return df


def fill_player_names_df(df, player_name, player_last_name, player_name_id, player_lastname_id,comment_id):
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
    df = pd.concat([df, pd.DataFrame([name_row],dtype=object)], ignore_index=True)
    df = pd.concat([df, pd.DataFrame([last_name_row],dtype=object)], ignore_index=True)
    return df

def fill_players_team_link(df, artificial_key, fifa_team_id, player_id, player_position):
    leaguegoals= 0
    isamongtopscorers = 0
    yellows = 0
    isamongtopscorersinteam = 0
    jerseynumber = random.randint(1, 50)
    position = player_position
    artificialkey = artificial_key
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

    df = pd.concat([df, pd.DataFrame([new_row],dtype=object)], ignore_index=True)
    return df


def create_players_from_club(tfk_club_id, fifa_club_id, starting_player_id, name_id_start_point):

    url = "http://localhost:8000/clubs/%s/players" % tfk_club_id
    response = requests.get(url)
    players_list = response.json()['players']
    firstnameid = name_id_start_point
    lastnameid = name_id_start_point+1
    playerid = starting_player_id

    player_data_frame = player_init_df()
    player_name_data_frame = player_name_df()
    player_team_data_frame = player_team_df()

    for player in players_list:
       if player['position'].replace("-", "").replace(" ", "").lower() in preferred_position_map:
            first_name, last_name = parse_name(player)
            player_data_frame = fill_player_dataframe(player_data_frame, player, lastnameid)
            player_name_data_frame = fill_player_names_df(player_name_data_frame,first_name,last_name,firstnameid,lastnameid,90000)
            player_team_data_frame = fill_players_team_link(player_team_data_frame,1,fifa_club_id,playerid,parse_preferred_position(player))
            firstnameid+= 2
            playerid+=1

    player_data_frame.to_csv('player.csv', index=False)
    player_name_data_frame.to_csv('player_names.csv', index=False)
    player_team_data_frame.to_csv('player_team.csv', index=False)


def parse_foot(player) -> int:
    if "foot" in player and player["foot"]:
       return foot_map[player["foot"]]
    else:
       return random.randint(1, 2)

def parse_second_foot(player) -> int:
    if "foot" in player and player["foot"]:
       return second_foot_map[player["foot"]]
    else:
       return random.randint(1, 5)

def parse_height(player) -> int:
    if "height" in player and player["height"]:
       return float(player["height"].replace("m","").replace(",","."))*100
    else:
       return random.randint(170, 190)


def parse_name(player) -> (str,str):
    name_parts = player['name'].split(' ', 1)
    if len(name_parts) == 2:
        return name_parts[0], name_parts[1]
    elif len(name_parts) == 1:
        return name_parts[0], ''
    else:
        return '', ''

def parse_preferred_position(player) -> int:
    preferred_position = player['position'].replace("-", "").replace(" ", "").lower()
    return preferred_position_map[preferred_position]

def parse_nationality(player) -> int:
    if "nationality" in player and player["nationality"]:
        nationality = player['nationality'][0]
        return nations_map[nationality]
    else:
        return nations_map["Free Agents Country"]

def parse_birth_date(player) -> int:
    reference_date = datetime(1970, 1, 1).timestamp()

    if "dateOfBirth" in player and player["dateOfBirth"]:
        date_string = player["dateOfBirth"]
        date_object = datetime.strptime(date_string, "%b %d, %Y")
        return date_object.timestamp() - reference_date
    else:
        random_date = datetime(year=random.randint(1989, 2002), month=random.randint(1, 12), day=random.randint(1, 28))
        return random_date.timestamp() - reference_date

def parse_joined_on(player) -> int:
    reference_date = datetime(1970, 1, 1).timestamp()

    if "joinedOn" in player and player["joinedOn"]:
        date_string = player["joinedOn"]
        date_object = datetime.strptime(date_string, "%b %d, %Y")
        return date_object.timestamp() - reference_date
    else:
        random_date = datetime(year=random.randint(2022, 2024), month=random.randint(1, 12), day=random.randint(1, 28))
        return random_date.timestamp() - reference_date

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    player_team_link_starting_point = 24674
    player_id_starting = 279949 #From this ID we start creating players
    player_name_id_starting = 29740
    team_fifa_id = 30079
    club_to_import_id_tfk = 30466

    create_players_from_club(club_to_import_id_tfk, team_fifa_id, player_name_id_starting, player_id_starting)
