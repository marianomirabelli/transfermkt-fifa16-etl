import mysql
import requests
import random
import pandas as pd
from sqlalchemy import create_engine
import pymysql

foot_map = {
    'right': 1,
    'left': 2,
    'both': lambda: random.randint(1,2)
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

#preferred_position_map = {
#    'none': -1,
#    'defender': random.choice([5,7,3]),
#    'midfielder': random.choice([10,14]),
#    'striker': random.choice([21,25])
#}

continent_map = {
    "Albania": "Europe",
    "Andorra": "Europe",
    "Armenia": "Asia",
    "Austria": "Europe",
    "Azerbaijan": "Asia",
    "Belarus": "Europe",
    "Belgium": "Europe",
    "Bosnia-Herzegovina": "Europe",
    "Bulgaria": "Europe",
    "Croatia": "Europe",
    "Cyprus": "Asia",
    "Czech Republic": "Europe",
    "Denmark": "Europe",
    "England": "Europe",
    "Montenegro": "Europe",
    "Faroe Islands": "Europe",
    "Finland": "Europe",
    "France": "Europe",
    "North Macedonia": "Europe",
    "Georgia": "Asia",
    "Germany": "Europe",
    "Greece": "Europe",
    "Hungary": "Europe",
    "Iceland": "Europe",
    "Republic of Ireland": "Europe",
    "Israel": "Asia",
    "Italy": "Europe",
    "Latvia": "Europe",
    "Liechtenstein": "Europe",
    "Lithuania": "Europe",
    "Luxembourg": "Europe",
    "Malta": "Europe",
    "Moldova": "Europe",
    "Netherlands": "Europe",
    "Northern Ireland": "Europe",
    "Norway": "Europe",
    "Poland": "Europe",
    "Portugal": "Europe",
    "Romania": "Europe",
    "Russia": "Europe",
    "San Marino": "Europe",
    "Scotland": "Europe",
    "Slovakia": "Europe",
    "Slovenia": "Europe",
    "Spain": "Europe",
    "Sweden": "Europe",
    "Switzerland": "Europe",
    "Turkey": "Asia",
    "Ukraine": "Europe",
    "Wales": "Europe",
    "Serbia": "Europe",
    "Argentina": "South America",
    "Bolivia": "South America",
    "Brazil": "South America",
    "Chile": "South America",
    "Colombia": "South America",
    "Ecuador": "South America",
    "Paraguay": "South America",
    "Peru": "South America",
    "Uruguay": "South America",
    "Venezuela": "South America",
    "Anguilla": "North America",
    "Antigua and Barbuda": "North America",
    "Aruba": "North America",
    "Bahamas": "North America",
    "Barbados": "North America",
    "Belize": "North America",
    "Bermuda": "North America",
    "British Virgin Islands": "North America",
    "Canada": "North America",
    "Cayman Islands": "North America",
    "Costa Rica": "North America",
    "Cuba": "North America",
    "Dominica": "North America",
    "Dominican Republic": "North America",
    "El Salvador": "North America",
    "Grenada": "North America",
    "Guatemala": "North America",
    "Guyana": "South America",
    "Haiti": "North America",
    "Honduras": "North America",
    "Jamaica": "North America",
    "Mexico": "North America",
    "Montserrat": "North America",
    "Curaçao": "North America",
    "Nicaragua": "North America",
    "Panama": "North America",
    "Puerto Rico": "North America",
    "St. Kitts and Nevis": "North America",
    "St. Lucia": "North America",
    "St. Vincent and the Grenadines": "North America",
    "Suriname": "South America",
    "Trinidad and Tobago": "North America",
    "Turks and Caicos Islands": "North America",
    "United States": "North America",
    "US Virgin Islands": "North America",
    "Tunisia": "Arabia",
    "Angola": "Africa",
    "Benin": "Africa",
    "Botswana": "Africa",
    "Botsuana": "Africa",
    "Burkina Faso": "Africa",
    "Burundi": "Africa",
    "Cameroon": "Africa",
    "Cape Verde Islands": "Africa",
    "Central African Republic": "Africa",
    "Chad": "Africa",
    "Congo": "Africa",
    "Cote d'Ivoire": "Africa",
    "Djibouti": "Africa",
    "Congo DR": "Africa",
    "DR Congo": "Africa",
    "Egypt": "Arabia",
    "Equatorial Guinea": "Africa",
    "Eritrea": "Africa",
    "Ethiopia": "Africa",
    "Gabon": "Africa",
    "Gambia": "Africa",
    "The Gambia": "Africa",
    "Ghana": "Africa",
    "Guinea": "Africa",
    "Guinea-Bissau": "Africa",
    "Kenya": "Africa",
    "Lesotho": "Africa",
    "Liberia": "Africa",
    "Libya": "Africa",
    "Madagascar": "Africa",
    "Malawi": "Africa",
    "Mali": "Africa",
    "Mauritania": "Africa",
    "Mauritius": "Africa",
    "Morocco": "Arabia",
    "Mozambique": "Africa",
    "Namibia": "Africa",
    "Niger": "Africa",
    "Nigeria": "Africa",
    "Rwanda": "Africa",
    "São Tomé e Príncipe": "Africa",
    "Senegal": "Africa",
    "Seychelles": "Africa",
    "Sierra Leone": "Africa",
    "Somalia": "Africa",
    "South Africa": "Africa",
    "Sudan": "Africa",
    "Eswatini": "Africa",
    "Tanzania": "Africa",
    "Togo": "Africa",
    "Algeria": "Arabia",
    "Uganda": "Africa",
    "Zambia": "Africa",
    "Zimbabwe": "Africa",
    "Afghanistan": "Asia",
    "Bahrain": "Asia",
    "Bangladesh": "Asia",
    "Bhutan": "Asia",
    "Brunei Darussalam": "Asia",
    "Cambodia": "Asia",
    "China PR": "Asia",
    "Guam": "Oceania",
    "Hong Kong": "Asia",
    "India": "Asia",
    "Indonesia": "Asia",
    "Iran": "Asia",
    "Iraq": "Asia",
    "Japan": "Asia",
    "Jordan": "Arabia",
    "Kazakhstan": "Asia",
    "Korea DPR": "Asia",
    "Korea Republic": "Asia",
    "Kuwait": "Asia",
    "Kyrgyzstan": "Asia",
    "Laos": "Asia",
    "Lebanon": "Asia",
    "Macau": "Asia",
    "Malaysia": "Asia",
    "Maldives": "Asia",
    "Mongolia": "Asia",
    "Myanmar": "Asia",
    "Nepal": "Asia",
    "Oman": "Asia",
    "Pakistan": "Asia",
    "Palestine": "Asia",
    "Philippines": "Asia",
    "Qatar": "Arabia",
    "Saudi Arabia": "Asia",
    "Singapore": "Asia",
    "Sri Lanka": "Asia",
    "Syria": "Asia",
    "Tajikistan": "Asia",
    "Thailand": "Asia",
    "Turkmenistan": "Asia",
    "United Arab Emirates": "Asia",
    "Uzbekistan": "Asia",
    "Vietnam": "Asia",
    "Yemen": "Asia",
    "American Samoa": "Oceania",
    "Australia": "Oceania",
    "Cook Islands": "Oceania",
    "Fiji": "Oceania",
    "New Zealand": "Oceania",
    "Papua New Guinea": "Oceania",
    "Samoa": "Oceania",
    "Solomon Islands": "Oceania",
    "Tahiti": "Oceania",
    "Tonga": "Oceania",
    "Vanuatu": "Oceania",
    "Gibraltar": "Europe",
    "Greenland": "North America",
    "Estonia": "Europe",
    "Timor-Leste": "Asia",
    "Chinese Taipei": "Asia",
    "Comoros": "Africa",
    "New Caledonia": "Oceania",
    "South Sudan": "Africa",
    "Kosovo": "Europe",
}

# Handle special cases or unknowns:
continent_map["International Men"] = "International"
continent_map["International Women"] = "International"
continent_map["Created Players Country"] = "Other"
continent_map["Free Agents Country"] = "Other"
continent_map["Rest of World"] = "Other"
continent_map["Creation Centre Country"] = "Other"


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
    'Botsuana': 100,
    'Burkina Faso': 101,
    'Burundi': 102,
    'Cameroon': 103,
    'Cape Verde Islands': 104,
    'Central African Republic': 105,
    'Chad': 106,
    'Congo': 107,
    "Cote d'Ivoire": 108,
    'Djibouti': 109,
    'Congo DR': 110,
    "DR Congo": 110,
    'Egypt': 111,
    'Equatorial Guinea': 112,
    'Eritrea': 113,
    'Ethiopia': 114,
    'Gabon': 115,
    'Gambia': 116,
    'The Gambia': 116,
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

##Defensive Skills
def marking_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(7, 15),
        **{pos: lambda: random.randint(55, 70) for pos in [3, 5, 7, 10, 14]},
        **{pos: lambda: random.randint(40, 55) for pos in [12, 16, 18, 27, 23]},
        **{pos: lambda: random.randint(40, 50) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def standingtackle_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(7, 18),
        **{pos: lambda: random.randint(55, 70) for pos in [5, 3, 7, 10, 14]},
        **{pos: lambda: random.randint(40, 55) for pos in [18, 12, 16]},
        **{pos: lambda: random.randint(25, 40) for pos in [27, 23, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def slidingtackle_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(7, 18),
        **{pos: lambda: random.randint(55, 70) for pos in [5, 3, 7, 10, 14]},
        **{pos: lambda: random.randint(40, 55) for pos in [18, 12, 16]},
        **{pos: lambda: random.randint(25, 40) for pos in [27, 23, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def aggression_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(20, 45),
        **{pos: lambda: random.randint(55, 75) for pos in [3, 5, 7, 10, 14]},
        **{pos: lambda: random.randint(40, 55) for pos in [12, 16, 18, 27, 23]},
        **{pos: lambda: random.randint(50, 60) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def interceptions_value (preferred_position) -> int:
    options =  {
        -1: -1,
        0: random.randint(7, 18),
        5 or 3 or 7 or 10 or 14 : random.randint(55, 70),
        18 or 12 or 16: random.randint(40, 55),
        27 or 23 or 21 or 25: random.randint(25, 40)
    }
    return options.get(preferred_position)

##Midfield Skills
def shortpassing_value(preferred_position) -> int:
    options = {
        -1:-1,
        0: random.randint(20, 35),
        3 or 5 or 7: random.randint(45, 62),
        10 or 14 or 18: random.randint(58, 75),
        12 or 16 or 27 or 23: random.randint(45, 65),
        21 or 25: random.randint(50, 70)
    }

    return options.get(preferred_position)

def interceptions_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(7, 18),
        **{pos: lambda: random.randint(55, 70) for pos in [5, 3, 7, 10, 14]},
        **{pos: lambda: random.randint(40, 55) for pos in [18, 12, 16]},
        **{pos: lambda: random.randint(25, 40) for pos in [27, 23, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def shortpassing_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(20, 35),
        **{pos: lambda: random.randint(45, 62) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(58, 75) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(45, 65) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(50, 70) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def longpassing_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(20, 35),
        **{pos: lambda: random.randint(25, 45) for pos in [5, 3, 7]},
        **{pos: lambda: random.randint(58, 70) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(55, 65) for pos in [12, 16]},
        **{pos: lambda: random.randint(40, 58) for pos in [27, 23, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def crossing_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(7, 15),
        5: lambda: random.randint(25, 40),
        **{pos: lambda: random.randint(58, 70) for pos in [3, 7]},
        **{pos: lambda: random.randint(55, 65) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(58, 70) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(45, 65) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def ballcontrol_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(15, 28),
        **{pos: lambda: random.randint(45, 60) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(55, 70) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(45, 65) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(45, 65) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def vision_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(4, 15),
        **{pos: lambda: random.randint(25, 35) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(55, 70) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(45, 55) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(35, 55) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def curve_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(10, 28),
        **{pos: lambda: random.randint(55, 75) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(45, 60) for pos in [10, 12, 14, 16, 27, 23]},
        18: lambda: random.randint(58, 75),
        **{pos: lambda: random.randint(45, 68) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

#Mental Skills

def positioning_value (preferred_position) -> int:
    return random.randint(20, 35) if preferred_position == 0 else random.randint(45, 70)

def potential_value (preferred_position) -> int:
    return random.randint(65, 75)

def attacking_work_rate(preferred_position) -> int:
    options = {
        -1: -1,
        **{pos: 1 for pos in [0, 5, 10, 14]},
        **{pos: 0 for pos in [3, 7, 12, 16]},
        **{pos: 2 for pos in [18, 27, 23, 21, 25]}
    }
    return options.get(preferred_position, -1)

def defensive_work_rate(preferred_position) -> int:
    options = {
        -1: -1,
        **{pos: 2 for pos in [0, 5, 10, 14]},
        **{pos: 0 for pos in [3, 7, 12, 16]},
        **{pos: 1 for pos in [18, 27, 23, 21, 25]}
    }
    return options.get(preferred_position, -1)

#Attacking Skills

def shotpower_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(15, 30),
        **{pos: lambda: random.randint(40, 60) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(55, 72) for pos in [10, 14, 18, 12, 16]},
        **{pos: lambda: random.randint(45, 62) for pos in [27, 23]},
        **{pos: lambda: random.randint(58, 75) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def longshots_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(12, 25),
        **{pos: lambda: random.randint(25, 45) for pos in [5, 3, 7]},
        **{pos: lambda: random.randint(58, 70) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(48, 68) for pos in [12, 16]},
        **{pos: lambda: random.randint(58, 70) for pos in [27, 23, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def dribbling_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(4, 10),
        5: lambda: random.randint(15, 30),
        **{pos: lambda: random.randint(45, 65) for pos in [3, 7]},
        **{pos: lambda: random.randint(35, 55) for pos in [10, 14]},
        18: lambda: random.randint(55, 70),
        **{pos: lambda: random.randint(55, 70) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(50, 70) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def volley_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(12, 22),
        **{pos: lambda: random.randint(35, 50) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(45, 60) for pos in [10, 14]},
        **{pos: lambda: random.randint(45, 65) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(58, 70) for pos in [18, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def headingaccuracy_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(12, 22),
        **{pos: lambda: random.randint(35, 45) for pos in [3, 7, 10, 14]},
        **{pos: lambda: random.randint(35, 52) for pos in [12, 16, 18, 27, 23]},
        **{pos: lambda: random.randint(55, 70) for pos in [5, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def finishing_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(4, 15),
        **{pos: lambda: random.randint(20, 35) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(35, 55) for pos in [10, 14]},
        **{pos: lambda: random.randint(45, 58) for pos in [12, 16]},
        **{pos: lambda: random.randint(52, 65) for pos in [27, 23]},
        **{pos: lambda: random.randint(58, 72) for pos in [18, 21, 25]}
    }
    return options.get(preferred_position, lambda: None)()



#Physcical skills
def sprintspeed_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(20, 32),
        5: lambda: random.randint(45, 65),
        **{pos: lambda: random.randint(55, 70) for pos in [3, 7]},
        **{pos: lambda: random.randint(50, 65) for pos in [10, 14]},
        18: lambda: random.randint(52, 70),
        **{pos: lambda: random.randint(58, 75) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(52, 68) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def strength_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(50, 70),
        5: lambda: random.randint(55, 75),
        **{pos: lambda: random.randint(35, 55) for pos in [3, 7]},
        **{pos: lambda: random.randint(45, 65) for pos in [10, 14]},
        18: lambda: random.randint(40, 55),
        **{pos: lambda: random.randint(40, 55) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(55, 75) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def balance_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(45, 65),
        **{pos: lambda: random.randint(45, 65) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(55, 75) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(45, 65) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(45, 68) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def freekickaccuracy_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(25, 45),
        **{pos: lambda: random.randint(45, 55) for pos in [3, 5, 7]},
        **{pos: lambda: random.randint(55, 75) for pos in [10, 14, 18]},
        **{pos: lambda: random.randint(45, 68) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(45, 65) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()


def acceleration_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(10, 25),
        5: lambda: random.randint(45, 62),
        **{pos: lambda: random.randint(58, 75) for pos in [3, 7]},
        **{pos: lambda: random.randint(50, 65) for pos in [10, 14]},
        18: lambda: random.randint(55, 68),
        **{pos: lambda: random.randint(58, 75) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(52, 68) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def jumping_value(preferred_position) -> int:
    options = {
        -1: -1,
        0: lambda: random.randint(55, 75),
        5: lambda: random.randint(55, 75),
        **{pos: lambda: random.randint(45, 55) for pos in [3, 7, 10, 14]},
        18: lambda: random.randint(45, 60),
        **{pos: lambda: random.randint(45, 55) for pos in [12, 16, 27, 23]},
        **{pos: lambda: random.randint(58, 75) for pos in [21, 25]}
    }
    return options.get(preferred_position, lambda: None)()

def player_head_type_code(nationality):
    if (continent_map[nationality] == "Arabia"):
        return random.choice([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
                             501, 502, 504, 506, 507, 508, 509, 513, 515, 516, 518, 519, 521, 522, 525, 526, 527, 528, 529, 530, 532,
                             1000, 1001, 1002, 1003, 1004, 1006, 1007, 1009, 1010, 1011, 1012, 1013, 1014, 1016, 1017, 1019, 1023, 1024, 1025,
                             1500, 1501, 1502, 1503, 1504, 1505, 1506, 1507, 1508, 1509, 1510, 1511, 1512, 1513, 1514, 1515, 1516, 1517,
                             1518, 1519, 1520, 1521, 1522, 1523, 1524, 1525, 1526, 1527, 1528,
                             2000, 2001, 2002, 2003, 2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014,
                             2015, 2016, 2017, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2029, 2030,
                             2500, 2501, 2502, 2503, 2504, 2505, 2506, 2507, 2508, 2509, 2510, 2511, 2512, 2513, 2514, 2515, 2516, 2517, 2518,
                             3000, 3001, 3005, 3500, 3501, 3502, 3503, 3504, 3505, 4000, 4001, 4002, 4003, 4500, 4502, 4525,  5000, 5001, 5002, 5003])
    elif (continent_map[nationality] == "Africa"):
        face_random_index = random.randint(0, len(face_style_map[1]()) - 1)
        face_style_map[1]()[face_random_index]
    elif (continent_map[nationality] == "Asia"):
        face_random_index = random.randint(0, len(face_style_map[2]()) - 1)
        face_style_map[2]()[face_random_index]
    elif (continent_map[nationality] == "Europa"):
        face_random_index = random.randint(0, len(face_style_map[3]()) - 1)
        face_style_map[3]()[face_random_index]

def player_skin_tone_code(nationality):
    if (continent_map[nationality] == "Arabia"):
        return random.choice([1,2,3,4,5,6,8,9,10])
    else:
        return random.choice([9,10])


def fill_player_dataframe(df,player,player_id, first_name_id,lastnameid, ponderation):

        preferred_position = parse_preferred_position(player)
        face_random_index = random.randint(0, len(face_style_map[1]()) - 1)
        shoetypecode = random.randint(0, 255)
        haircolorcode = random.choice([0, 1, 2, 3, 4, 5, 6, 7, 9])  ##Negro
        facialhairtypecode = random.randint(0, 15)
        curve = random.randint(40, 66)
        jerseystylecode = 0  # Normal
        agility = random.randint(50, 68)
        accessorycode4 = 0
        gksavetype = (random.randint(0, 1) if preferred_position == 0 else 0) + ponderation()
        positioning = positioning_value(preferred_position) + ponderation()
        hairtypecode = random.randint(0, 124)
        standingtackle = standingtackle_value(preferred_position) + ponderation()
        faceposercode = 0
        preferredposition3 = -1
        longpassing = longpassing_value(preferred_position) + ponderation()
        penalties = random.randint(35, 70) + ponderation()
        animfreekickstartposcode = random.randint(0, 1)
        animpenaltieskickstylecode = random.randint(0, 2)
        isretiring = 0
        longshots = longshots_value(preferred_position) + ponderation()
        gkdiving = (random.randint(55, 66) if preferred_position == 0 else random.randint(4, 16)) + ponderation()
        interceptions = interceptions_value(preferred_position) + ponderation()
        shoecolorcode2 = random.randint(0, 31)
        crossing = crossing_value(preferred_position) + ponderation()
        potential = potential_value(preferred_position) + ponderation()
        gkreflexes = (random.randint(55, 66) if preferred_position == 0 else random.randint(4, 12)) + ponderation()
        finishingcode1 = 0
        reactions = (random.randint(55, 70) if preferred_position == 0 else random.randint(40, 55)) + ponderation()
        vision = vision_value(preferred_position) + ponderation()
        contractvaliduntil = random.randint(2025, 2027)
        finishing = finishing_value(preferred_position) + ponderation()
        dribbling = dribbling_value(preferred_position) + ponderation()
        slidingtackle = slidingtackle_value(preferred_position) + ponderation()
        accessorycode3 = 0
        accessorycolourcode1 = random.randint(0, 1)
        headtypecode = player_head_type_code(player['nationality'][0])  # Give a random face based on african or arabic styles
        firstnameid = first_name_id
        sprintspeed = sprintspeed_value(preferred_position) + ponderation()
        height = parse_height(player)
        hasseasonaljersey = random.randint(0, 4)
        preferredposition2 = -1
        strength = strength_value(preferred_position) + ponderation()
        birthdate = parse_birth_date(player)  ## TODO: Fix date format
        preferredposition1 = preferred_position
        ballcontrol = ballcontrol_value(preferred_position) + ponderation()
        shotpower = shotpower_value(preferred_position) + ponderation()
        trait1 = 0
        socklengthcode = random.randint(0, 1)
        weight = random.randint(65, 85)
        hashighqualityhead = 0
        gkglovetypecode = random.randint(0, 127)
        balance = balance_value(preferred_position) + ponderation()
        gender = 0
        gkkicking = (random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)) + ponderation()
        internationalrep = random.randint(1, 3)
        animpenaltiesmotionstylecode = random.randint(0, 1)
        shortpassing = shortpassing_value(preferred_position) + ponderation()
        freekickaccuracy = freekickaccuracy_value(preferred_position) + ponderation()
        skillmoves = 0 if preferred_position == 0 else random.randint(0, 2)
        usercaneditname = 0
        attackingworkrate = attacking_work_rate(preferred_position)
        finishingcode2 = 0
        aggression = aggression_value(preferred_position) + ponderation()
        acceleration = acceleration_value(preferred_position) + ponderation()
        headingaccuracy = headingaccuracy_value(preferred_position) + ponderation()
        eyebrowcode = random.randint(0, 1)
        runningcode2 = random.randint(0, 127)
        gkhandling = (random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)) + ponderation()
        eyecolorcode = 3
        jerseysleevelengthcode = 0
        accessorycolourcode3 = 0
        accessorycode1 = 0
        playerjointeamdate = parse_joined_on(player)  ## TODO: Fix date format
        headclasscode = 1  ## 1 for Africans and Arabics
        defensiveworkrate = defensive_work_rate(preferred_position)
        nationality = parse_nationality(player)
        preferredfoot = parse_foot(player)
        sideburnscode = 0
        weakfootabilitytypecode = parse_second_foot(player)
        jumping = jumping_value(preferred_position) + ponderation()
        skintypecode = random.randint(0, 2)
        gkkickstyle = 0 if preferred_position == 0 else random.randint(0, 3)
        stamina = (random.randint(60, 75) if preferred_position == 0 else random.randint(52, 78)) + ponderation()
        playerid = player_id
        marking = marking_value(preferred_position)
        accessorycolourcode4 = 0
        gkpositioning = (random.randint(50, 70) if preferred_position == 0 else random.randint(4, 11)) + ponderation()
        trait2 = 0
        skintonecode = player_skin_tone_code(player['nationality'][0]) # Give a random between african and arabic styles
        shortstyle = 0
        overallrating = random.randint(55, 70) + ponderation()
        emotion = random.randint(1, 5)
        jerseyfit = 0
        accessorycode2 = 0
        shoedesigncode = 0
        playerjerseynameid = lastnameid  ## TODO: check if this id comes from players_names file
        shoecolorcode1 = 30
        commonnameid = 0
        bodytypecode = random.randint(1, 7)
        animpenaltiesstartposcode = random.randint(0, 2)
        runningcode1 = 0
        preferredposition4 = -1
        volleys = volley_value(preferred_position) + ponderation()
        accessorycolourcode2 = 0
        facialhaircolorcode = random.choice([0,2,3,4]) #Negro

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

def fill_player_dlc_names_df(df, player_name, player_last_name, player_name_id, player_lastname_id):
    name_row = {
        'name': player_name,
        'nameId': player_name_id
    }

    last_name_row = {
        'name': player_last_name,
        'nameId': player_lastname_id
    }
    df = pd.concat([df, pd.DataFrame([name_row],dtype=object)], ignore_index=True)
    df = pd.concat([df, pd.DataFrame([last_name_row],dtype=object)], ignore_index=True)
    return df

def fill_players_team_link(df, artificial_key, fifa_team_id, player_id, player_position,artificial_id):
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
        'artificialkey': artificial_id,
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


def create_players_from_club(club_name, tfk_club_id, fifa_club_id, starting_player_id, name_id_start_point,artificial_id,pondeartion):

    url = "http://localhost:8000/clubs/%s/players" % tfk_club_id
    response = requests.get(url)
    players_list = response.json()['players']
    firstnameid = name_id_start_point
    lastnameid = firstnameid+1
    playerid = starting_player_id

    player_data_frame = player_init_df()
    player_name_data_frame = player_name_df()
    player_name_dlc_data_frame = player_name_dlc_df()
    player_team_data_frame = player_team_df()

    engine = create_engine("mysql+pymysql://root@localhost:3306/fifa16")


    for player in players_list:
       if player['position'].replace("-", "").replace(" ", "").lower() in preferred_position_map:
            first_name, last_name = parse_name(player)
            player_data_frame = fill_player_dataframe(player_data_frame, player, playerid,firstnameid,lastnameid,pondeartion)
            player_name_data_frame = fill_player_names_df(player_name_data_frame,first_name,last_name,firstnameid,lastnameid,900000)
            player_name_dlc_data_frame = fill_player_dlc_names_df(player_name_dlc_data_frame,first_name,last_name,firstnameid,lastnameid)
            player_team_data_frame = fill_players_team_link(player_team_data_frame,1,fifa_club_id,playerid,parse_preferred_position(player),artificial_id)
            artificial_id+=1
            firstnameid += 2
            lastnameid= firstnameid+1
            playerid+=1

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

    player_data_frame.to_sql('players',engine,if_exists='append',index=False)
    player_name_data_frame.to_sql('playernames', engine, if_exists='append',index=False)
    player_name_dlc_data_frame.to_sql('dcplayernames',engine, if_exists='append',index=False)
    player_team_data_frame.to_sql('teamplayerlinks',engine, if_exists='append',index=False)

    print("Next Player Id is : %s\n"%playerid)
    print("Next Name Id is : %s\n"%firstnameid)
    print("Next Artificial Key Id is : %s\n" % artificial_id)


def parse_foot(player) -> int:
    if "foot" in player and player["foot"] and player["foot"] != 'both':
       return foot_map[player["foot"]]
    else:
       return random.randint(1, 2)

def parse_second_foot(player) -> int:
    if "foot" in player and player["foot"]:
       return second_foot_map[player["foot"]]()
    else:
       return random.randint(1, 5)

def parse_height(player) -> int:
    if "height" in player and player["height"]:
       return int(float(player["height"].replace("m","").replace(",","."))*100)
    else:
       return random.randint(160, 190)


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
   return random.randint(148368,154211)

def parse_joined_on(player) -> int:
    return random.randint(160237, 160997)

# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    player_id_starting = 281117
    player_name_id_starting = 30753
    artificial_id = 25375
    team_fifa_id = 30072
    club_to_import_id_tfk = 22944
    team_name = "Safi"

    create_players_from_club(team_name, club_to_import_id_tfk, team_fifa_id, player_id_starting,player_name_id_starting,artificial_id,lambda:random.randint(5,7))

# FC Taraba matches with Plateau United. Test with Katsina United if possible
# Gombe United matches Sharks FC