import json
import os
import psycopg2
from datetime import datetime


# Function to read JSON data from a file
def read_json_file(file_path):
    assert os.path.isfile(file_path), f"{file_path} is not a file."
    with open(file_path, 'r', encoding='utf-8') as file:
        return json.load(file)


# Connect to PostgreSQL database
conn = psycopg2.connect(
    dbname='project_database',
    user='postgres',
    password='12345',
    host='localhost',
    port='5432'
)

# Create a cursor object
cur = conn.cursor()

# Define the directory paths
data_dir = 'C:/Users/HP/Downloads/open-data/data/'

# Read competitions.json file
competitions_data = read_json_file(os.path.join(data_dir, 'competitions.json'))

# Insert data from competitions.json
for competition in competitions_data:

    competition_id = competition['competition_id']
    season_id = competition['season_id']
    country_name = competition['country_name']
    competition_name = competition['competition_name']
    competition_gender = competition['competition_gender']
    competition_youth = competition['competition_youth']
    competition_international = competition['competition_international']
    season_name = competition['season_name']
    match_updated = competition['match_updated']
    match_updated_360 = competition.get('match_updated_360')
    match_available_360 = competition.get('match_available_360')
    match_available = competition.get('match_available')
    
    cur.execute("""
        INSERT INTO competitions (competition_id, season_id, country_name, competition_name,
                                 competition_gender, competition_youth, competition_international,
                                 season_name, match_updated, match_updated_360, match_available_360,
                                 match_available)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (competition_id) DO NOTHING;
    """, (competition_id, season_id, country_name, competition_name, competition_gender,
          competition_youth, competition_international, season_name, match_updated,
          match_updated_360, match_available_360, match_available))
    
#print("after insert competition")
# Function to get target competition IDs
def get_target_competition_ids():
    # Read competitions.json file
    competitions_data = read_json_file(os.path.join(data_dir, 'competitions.json'))
    
    target_ids = set()
    for competition in competitions_data:
        if (competition['competition_name'] == 'La Liga' and competition['season_name'] in ['2018/2019','2019/2020', '2020/2021']):
            target_ids.add((competition['competition_id'], competition['season_id']))
        if (competition['competition_name'] == 'Premier League' and 
              competition['season_name'] == '2003/2004'):
            target_ids.add((competition['competition_id'], competition['season_id']))
    
    return target_ids
#print("Function get_after target_competition_ids")
# Get target competition IDs
target_competitions = get_target_competition_ids()
print(f'get_target_competition_ids return values : {target_competitions}')
#print("after target_competitions")
# Extract data from matches for all tables
for competition_id, season_id in get_target_competition_ids():
    # print('inside for-loop')
    #competition_id = int(competition_id)  # Convert to integer
    if (competition_id,season_id) in target_competitions:
        #print(f"Processing matches for competition ID: {competition_id}, season ID: {season_id}")
        season_files_path = os.path.join(data_dir, 'matches', str(competition_id), str(season_id)+ ".json")
        #print("Season Files Path:", season_files_path)
         # Read the JSON file
        match_data = read_json_file(season_files_path)
        for match_data_entry in match_data:
                    # print(match_data_entry)
                    try:
                        # Extract match data
                        match_id = match_data_entry['match_id']
                        match_date = match_data_entry['match_date']
                        kick_off = match_data_entry['kick_off']
                        home_score = match_data_entry['home_score']
                        away_score = match_data_entry['away_score']
                        match_status = match_data_entry['match_status']
                        match_status_360 = match_data_entry['match_status_360']
                        last_updated = match_data_entry['last_updated']
                        last_updated_360=match_data_entry['last_updated_360']
                        match_week = match_data_entry['match_week']

                        # Extract competition data
                        competition_id = match_data_entry['competition']['competition_id']
                        country_name = match_data_entry['competition']['country_name']
                        competition_name = match_data_entry['competition']['competition_name']

                        # Extract season data
                        season_id = match_data_entry['season']['season_id']
                        season_name = match_data_entry['season']['season_name']

                        # Extract home team data
                        home_team_id = match_data_entry['home_team']['home_team_id']
                        home_team_name = match_data_entry['home_team']['home_team_name']
                        home_team_gender = match_data_entry['home_team']['home_team_gender']
                        home_team_group = match_data_entry['home_team']['home_team_group']

                        # Extract home team country data
                        home_team_id= match_data_entry ['home_team'] ['home_team_id']
                        home_team_country_id= match_data_entry ['home_team'] ['country'] ['id']
                        home_team_country_name=match_data_entry ['home_team'] ['country']['name']

                        # Extract home team manager data
                        if 'managers' in match_data_entry['home_team'] and match_data_entry['home_team']['managers']:
                            home_team_id= match_data_entry ['home_team'] ['home_team_id']
                            home_team_manager_id = match_data_entry['home_team']['managers'][0]['id']
                            home_team_manager_name = match_data_entry['home_team']['managers'][0]['name']
                            home_team_manager_nickname = match_data_entry['home_team']['managers'][0]['nickname']
                            home_team_manager_dob = match_data_entry['home_team']['managers'][0]['dob']
                        else:
                            home_team_id= None
                            home_team_manager_id = None
                            home_team_manager_name = None
                            home_team_manager_nickname = None
                            home_team_manager_dob = None
                        # Extract home team manager country data
                        home_team_manager_id= match_data_entry ['home_team'] ['managers'][0]['id']
                        home_team_manager_country_id= match_data_entry ['home_team'] ['managers'][0]['country']['id']
                        home_team_manager_country_name= match_data_entry ['home_team'] ['managers'][0]['country'] ['name']
                        # Extract away team data
                        away_team_id = match_data_entry['away_team']['away_team_id']
                        away_team_name = match_data_entry['away_team']['away_team_name']
                        away_team_gender = match_data_entry['away_team']['away_team_gender']
                        away_team_group = match_data_entry['away_team']['away_team_group']

                        # Extract away team country data
                        away_team_id= match_data_entry ['away_team'] ['away_team_id']
                        away_team_country_id= match_data_entry ['away_team']  ['country'] ['id']
                        away_team_country_name=match_data_entry ['away_team'] ['country']['name']
                        # Extract away team manager data
                        if 'managers' in match_data_entry['away_team'] and match_data_entry['away_team']['managers']:
                            away_team_id= match_data_entry ['away_team'] ['away_team_id']
                            away_team_manager_id = match_data_entry['away_team']['managers'][0]['id']
                            away_team_manager_name = match_data_entry['away_team']['managers'][0]['name']
                            away_team_manager_nickname = match_data_entry['away_team']['managers'][0]['nickname']
                            away_team_manager_dob = match_data_entry['away_team']['managers'][0]['dob']
                        else:
                            away_team_manager_id = None
                            away_team_manager_name = None
                            away_team_manager_nickname = None
                            away_team_manager_dob = None

                        # Extract home team manager country data
                        away_team_manager_id= match_data_entry ['away_team'] ['managers'] [0] ['id']
                        away_team_manager_country_id= match_data_entry ['away_team'] ['managers'] [0]['country'] ['id']
                        away_team_manager_country_name= match_data_entry ['away_team'] ['managers'] [0]['country'] ['name']

                        # Extract metadata data
                        data_version= match_data_entry ['metadata'].get('data_version')
                        shot_fidelity_version=match_data_entry ['metadata'].get('shot_fidelity_version')
                        xy_fidelity_version=match_data_entry ['metadata'].get('xy_fidelity_version', None)
                        # Extract competition_stage data
                        id=match_data_entry['competition_stage'] ['id']
                        name=match_data_entry['competition_stage'] ['name']

                        # Extract stadium data
                        if 'stadium' in match_data_entry:
                            match_id=match_data_entry['match_id']
                            stadium_id = match_data_entry['stadium'].get('id', -1)
                            name = match_data_entry['stadium'].get('name')
                            if 'country' in match_data_entry['stadium']:
                                    stadium_id = match_data_entry['stadium'].get('id', -1)
                                    stadium_country_id = match_data_entry['stadium']['country'].get('id', -1)
                                    stadium_country_name = match_data_entry['stadium']['country'].get('name')
                            else:
                                stadium_country_id = -1
                                stadium_country_name = None
                        else:
                            stadium_id = -1
                            name = None
                           


                        # Extract referee data
                        if 'referee' in match_data_entry:
                            match_id=match_data_entry['match_id']
                            referee_id = match_data_entry['referee'].get('id', -1)  # Use -1 as a default value
                            referee_name = match_data_entry['referee'].get('name')
                        else:
                            referee_id = -1
                            referee_name = None

                        # Extract referee country data
                        if 'referee' in match_data_entry and 'country' in match_data_entry['referee']:
                            referee_country_id = match_data_entry['referee']['country'].get('id', -1)
                            referee_country_name = match_data_entry['referee']['country'].get('name')
                        else:
                            referee_country_id = -1
                            referee_country_name = None
                    except KeyError as e:

                        #print(f"KeyError: '{e.args[0]}' not found in match data.")
                        continue

                    # Insert data into the match table

                    cur.execute("""
                        INSERT INTO match (
                            match_id, match_date, kick_off, home_score, away_score,
                            match_status, match_status_360, last_updated, last_updated_360,
                            match_week
                        )
                        VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (match_id) DO NOTHING;
                    """, (
                        match_id, match_date, kick_off, home_score, away_score,
                        match_status, match_status_360, last_updated,last_updated_360, match_week
                    ))

                    cur.execute("""
                        INSERT INTO competition (
                            match_id, competition_id, country_name, competition_name
                        )
                        VALUES (
                            %s, %s, %s, %s
                        )
                        ON CONFLICT (competition_id) DO NOTHING;
                    """, (
                        match_id, competition_id, country_name, competition_name
                    ))

                    cur.execute("""
                        INSERT INTO season (
                            match_id, season_id, season_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (season_id) DO NOTHING;
                    """, (
                        match_id, season_id, season_name
                    ))

                    cur.execute("""
                        INSERT INTO home_team (
                            match_id, home_team_id, home_team_name, home_team_gender, home_team_group
                        )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (home_team_id) DO NOTHING;
                    """, (
                        match_id, home_team_id, home_team_name, home_team_gender, home_team_group
                    ))

                    cur.execute("""
                        INSERT INTO home_team_country(
                            home_team_id, home_team_country_id, home_team_country_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (home_team_country_id) DO NOTHING;
                    """, (
                        home_team_id, home_team_country_id, home_team_country_name
                    ))
                    cur.execute("""
                        INSERT INTO home_team_manager (
                            home_team_id, home_team_manager_id, home_team_manager_name, home_team_manager_nickname, home_team_manager_dob
                        )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (home_team_manager_id) DO NOTHING;
                    """, (
                        home_team_id, home_team_manager_id, home_team_manager_name, home_team_manager_nickname, home_team_manager_dob
                    ))

                    cur.execute("""
                        INSERT INTO home_team_manager_country(
                            home_team_manager_id, home_team_manager_country_id, home_team_manager_country_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (home_team_manager_country_id) DO NOTHING;
                        
                    """, (
                        home_team_manager_id, home_team_manager_country_id, home_team_manager_country_name
                    ))



                    cur.execute("""
                        INSERT INTO away_team (
                            match_id, away_team_id, away_team_name, away_team_gender, away_team_group
                        )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (away_team_id) DO NOTHING;
                    """, (
                        match_id, away_team_id, away_team_name, away_team_gender, away_team_group
                    ))

                    cur.execute("""
                        INSERT INTO away_team_country(
                            away_team_id, away_team_country_id, away_team_country_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (away_team_country_id) DO NOTHING;
                    """, (
                        away_team_id, away_team_country_id, away_team_country_name
                    ))
                    cur.execute("""
                        INSERT INTO away_team_manager (
                            away_team_id, away_team_manager_id, away_team_manager_name, away_team_manager_nickname, away_team_manager_dob
                        )
                        VALUES (
                            %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (away_team_manager_id) DO NOTHING;
                    """, (
                        away_team_id, away_team_manager_id, away_team_manager_name, away_team_manager_nickname, away_team_manager_dob
                    ))

                    cur.execute("""
                        INSERT INTO away_team_manager_country(
                            away_team_manager_id, away_team_manager_country_id, away_team_manager_country_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (away_team_manager_country_id) DO NOTHING;
                    """, (
                        away_team_manager_id, away_team_manager_country_id, away_team_manager_country_name
                    ))

                    cur.execute("""
                        INSERT INTO metadata(
                            match_id, data_version, shot_fidelity_version, xy_fidelity_version
                        )
                        VALUES (
                            %s, %s, %s, %s
                        )
                        
                    """, (
                        match_id, data_version,shot_fidelity_version ,xy_fidelity_version
                    ))
                    cur.execute("""
                        INSERT INTO competition_stage (
                            match_id, id, name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (id) DO NOTHING;
                    """, (
                        match_id, id, name
                    ))



                    cur.execute("""
                        INSERT INTO stadium (
                            match_id, stadium_id, name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (stadium_id) DO NOTHING;
                    """, (
                        match_id, stadium_id,name
                    ))

                    cur.execute("""
                        INSERT INTO stadium_country (
                            stadium_id, stadium_country_id, stadium_country_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (stadium_country_id) DO NOTHING;
                    """, (
                        stadium_id, stadium_country_id,stadium_country_name
                    ))
                    

                    cur.execute("""
                        INSERT INTO referee (
                            match_id, referee_id, referee_name
                        )
                        VALUES (
                            %s, %s, %s
                        )
                        ON CONFLICT (referee_id) DO NOTHING;
                    """, (
                        match_id, referee_id,referee_name
                    ))
                    # print("Referee ID:", referee_id)

                    cur.execute("""
                                INSERT INTO referee_country (
                                    referee_id, referee_country_id, referee_country_name
                                )
                                VALUES (
                                    %s, %s, %s
                                )
                                ON CONFLICT (referee_country_id) DO NOTHING;
                            """, (
                                referee_id, referee_country_id, referee_country_name
                            ))
                    

def get_match_ids_for_target_competitions():
    target_competitions = get_target_competition_ids()  # Assuming this function exists and returns target competitions
    match_ids = set()
    
    for competition_id, season_id in target_competitions:
        season_files_path = os.path.join(data_dir, 'matches', str(competition_id), f"{season_id}.json")
        match_data = read_json_file(season_files_path)
        
        for match_entry in match_data:
            match_ids.add(match_entry['match_id'])
    
    return match_ids

match_id_set=get_match_ids_for_target_competitions()

#print(f"match_ids:{match_id_set}")



# Insert data from events folder for La Liga 2018-2019, 2019-2020, 2020-2021 and Premier League 2003-2004
for match_id in match_id_set:
        #print('in first for-loop')
        events_file_path = os.path.join(data_dir, 'events', f"{match_id}.json")
        events_data = read_json_file(events_file_path)
        encountered_event_ids = set()
        for event_data_entry in events_data:
                #print('in second for-loop')
                try:
                    # Insert event data into the database
                    # # Extract events data
                    event_id = event_data_entry['id']
                    # index = event_data_entry['index']
                    # period = event_data_entry['period']
                    # timestamp_str = event_data_entry['timestamp']
                    # #Parse the timestamp string to datetime object
                    # timestamp_obj = datetime.strptime(timestamp_str, "%H:%M:%S.%f")
                    # # Convert the datetime object back to string in the desired format
                    # formatted_timestamp = timestamp_obj.strftime("%H:%M:%S.%f")[:-3] 
                    # minute = event_data_entry['minute']
                    # second = event_data_entry['second']
                    # possession = event_data_entry['possession']
                    # duration = event_data_entry.get('duration')
                    # location = event_data_entry.get('location', [])
                    # location_array = "{" + ",".join(str(coord) for coord in location) + "}"
                    # off_camera = event_data_entry.get('off_camera', False)
                    # under_pressure = event_data_entry.get('under_pressure', False)
                    # out = event_data_entry.get('out', False)
                    # #permanent=event_data_entry['permanent']
                    # recovery_failure=event_data_entry.get('ball_recovery',{}).get('recovery_failure', False)
                    # carry_end_location=event_data_entry.get('carry',{}).get('end_location',[])
                    # carry_end_location_array= "{" + ",".join(str(coord) for coord in carry_end_location) + "}"
                    # gk_end_location=event_data_entry.get('goalkeeper',{}).get('end_location',[])
                    # gk_end_location_array= "{" + ",".join(str(coord) for coord in gk_end_location) + "}"
                    # counterpress = event_data_entry.get('counterpress', False)
                    # related_events = event_data_entry.get('related_events', [])
                    # related_events_array = "{" + ",".join(event.strip("'") for event in related_events) + "}"
                    # formation= event_data_entry.get('tactics',{}).get('formation')
                
                    # # Extract event_type data
                    # event_type_id = event_data_entry['type']['id']
                    # event_type_name = event_data_entry['type']['name']
                    # # Extract possession_team data
                    # possession_team_id = event_data_entry['possession_team']['id']
                    # possession_team_name = event_data_entry['possession_team']['name']


                    # # Extract play_pattern data
                    
                    # play_pattern_id = event_data_entry['play_pattern']['id']
                    # play_pattern_name = event_data_entry['play_pattern']['name']

                    


                    # # Extract team data
                    # team_id = event_data_entry['team']['id']
                    # team_name = event_data_entry['team']['name']

                    # #Extract event_player data
                    # try:
                    #     event_player_id = event_data_entry['player']['id']
                    #     event_player_name = event_data_entry['player']['name']
                    # except KeyError:
                    # #     # Handle the case where 'player' information is not available
                    #      continue

                    # # Extract event_position data
                    # try:
                    #     event_position_id = event_data_entry['position']['id']
                    #     event_position_name = event_data_entry['position']['name']
                    # except KeyError:
                    #     continue
                        

                    # # Extract lineup_position data
                    # if 'lineup' in event_data_entry:
                    #     for position_data in event_data_entry.get('lineup', []):
                    #         lineup_position_id = position_data['position']['id']
                    #         lineup_position_name = position_data['position']['name']
                    #         print('lineup_position_id:', lineup_position_id)
                    #         print('lineup_position_name: ', lineup_position_name)
                    # Extract lineup_position data
                    # Extract lineup_position data
                    # if 'tactics' in event_data_entry:
                    #     tactics_data = event_data_entry['tactics']
                    #     if 'lineup' in tactics_data:
                    #         lineup_data = tactics_data['lineup']
                    #         # print("Lineup data found:", lineup_data)
                    #         for position_data in lineup_data:
                    #             lineup_position_id = position_data['position']['id']
                    #             lineup_position_name = position_data['position']['name']
                    #             lineup_player_id =position_data['player']['id']
                    #             lineup_player_name = position_data['player']['name']
                    #             jersey_number = position_data['jersey_number']
                    #             cur.execute("""
                    #             INSERT INTO lineup_position 
                    #             (event_id, lineup_position_id, lineup_position_name) 
                    #             VALUES 
                    #             (%s, %s, %s)
                    #             ON CONFLICT (lineup_position_id) DO NOTHING;
                    #             """, (
                    #             event_id, lineup_position_id, lineup_position_name
                    #             ))
                    #              # Insert data into the lineup_player table
                    #             cur.execute("""
                    #                 INSERT INTO lineup_player 
                    #                 (event_id, lineup_player_id, lineup_player_name) 
                    #                 VALUES 
                    #                 (%s, %s, %s)
                    #                 ON CONFLICT (lineup_player_id) DO NOTHING;
                    #             """, (
                    #                 event_id, lineup_player_id, lineup_player_name
                    #             ))
                    #             # Insert data into the lineup table
                    #             cur.execute("""
                    #                 INSERT INTO lineup (event_id, lineup_player_id, lineup_position_id, jersey_number)
                    #                 VALUES (%s, %s, %s, %s)
                    #             """, (event_id, lineup_player_id, lineup_position_id, jersey_number
                    #                 ))
                                        


                    # # Extract pass data
                    # pass_data = event_data_entry.get('pass', {})
                    # length = pass_data.get('length')
                    # angle = pass_data.get('angle')
                    # pass_end_location = pass_data.get('end_location', [])
                    # pass_end_location_array = "{" + ",".join(str(coord) for coord in pass_end_location) + "}"
                    # switch = pass_data.get('switch')
                    # cross = pass_data.get('cross')
                    # assisted_shot_id = pass_data.get('assisted_shot_id')
                    # backheel = pass_data.get('backheel')
                    # deflected = pass_data.get('deflected')
                    # miscommunication = pass_data.get('miscommunication')
                    # cutback = pass_data.get('cut-back')
                    # shot_assist = pass_data.get('shot_assist')
                    # goalassist = pass_data.get('goal-assist')
                    # pass_aerial_won = pass_data.get('pass_aerial_won')



                    # #Extract recipient data

                    # recipient_id=event_data_entry ['pass']['recipient']['id']
                    # recipient_name=event_data_entry ['pass']['recipient']['name']

                    # #Extract height data
                    # height_id=event_data_entry ['pass']['height']['id']
                    # height_name=event_data_entry ['pass']['height']['name']

                    # #Extract pass_body_part data
                    # pass_body_part_id=event_data_entry ['pass']['body_part']['id']
                    # pass_body_part_name=event_data_entry ['pass']['body_part']['name']
                    # #Extract pass_type data
                    # pass_type_id = pass_data['type']['id'] 
                    # pass_type_name = pass_data['type']['name'] 
                    # #Extract pass_outcome data
                    # pass_outcome_id=event_data_entry ['pass']['outcome']['id']
                    # pass_outcome_name=event_data_entry ['pass']['outcome']['name']
                    # #Extract pass_technique data
                    # pass_technique_id=event_data_entry ['pass']['technique']['id']
                    # pass_technique_name=event_data_entry ['pass']['technique']['name']

                    # Extract shot data
                    if 'shot' in event_data_entry:
                        shot_data = event_data_entry['shot']
                        key_pass_id = shot_data.get('key_pass_id')
                        statsbomb_xg = shot_data.get('statsbomb_xg', None)
                        shot_end_location = shot_data.get('end_location', [])
                        shot_end_location_array="{" + ",".join(str(coord) for coord in shot_end_location) + "}"
                        first_time = shot_data.get('first_time')
                        shot_aerial_won = shot_data.get('aerial_won', False)
                        follows_dribble = shot_data.get('follows_dribble', False)
                        open_goal = shot_data.get('open_goal', False)
                        deflected = shot_data.get('deflected', False)

                        # Extract freeze_frame data
                        freeze_frame_location = []
                        freeze_frame_teammate = False
                        if 'freeze_frame' in shot_data:
                            freeze_frame_data = shot_data['freeze_frame']
                            for frame in freeze_frame_data:
                                location = frame.get('location', [])
                                if len(location) == 2:
                                    freeze_frame_location.append(location)
                                if frame.get('teammate', False):
                                    freeze_frame_teammate = True

                        # Convert freeze_frame_location to array format
                        freeze_frame_location_array = "{" + ",".join("{" + ",".join(str(coord) for coord in frame) + "}" for frame in freeze_frame_location) + "}"

                        # Insert shot data into the database
                        cur.execute("""
                            INSERT INTO shot 
                            (event_id, key_pass_id, statsbomb_xg, shot_end_location, first_time, shot_aerial_won, follows_dribble, open_goal, deflected, freeze_frame_location, freeze_frame_teammate) 
                            VALUES 
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            
                        """, (
                            event_id, key_pass_id, statsbomb_xg, shot_end_location_array, first_time, shot_aerial_won, follows_dribble, open_goal, deflected, freeze_frame_location_array, freeze_frame_teammate
                        ))





                    # Extract shot_body_part data
                    shot_body_part_id=event_data_entry ['shot']['body_part']['id']
                    shot_body_part_name=event_data_entry ['shot']['body_part']['name']

                    # Extract shot_type data
                    shot_type_id=event_data_entry ['shot']['type']['id']
                    shot_type_name=event_data_entry ['shot']['type']['name']

                    # Extract shot_outcome data
                    shot_outcome_id=event_data_entry ['shot']['outcome']['id']
                    shot_outcome_name=event_data_entry ['shot']['outcome']['name']

                    # Extract shot_technique data
                    shot_technique_id=event_data_entry ['shot']['technique']['id']
                    shot_technique_name=event_data_entry ['shot']['technique']['name']

                    # Extract freeze_frame player data
                    freeze_frame_data = shot_data.get('freeze_frame', [])
                    if freeze_frame_data:
                        for frame in freeze_frame_data:
                            if 'player' in frame and 'id' in frame['player']:
                                shot_freeze_frame_player_id = frame['player']['id']
                                shot_freeze_frame_player_name = frame['player']['name']
                                break
                        for frame in freeze_frame_data:
                            if 'position' in frame and 'id' in frame['position']:
                                shot_freeze_frame_position_id = frame['position']['id']
                                shot_freeze_frame_position_name = frame['position']['name']
                                break
                                 

                    # Extract goalkeeper position data
              
                         
                    gk_position_id = event_data_entry['goalkeeper']['position']['id']
                    gk_position_name = event_data_entry['goalkeeper']['position']['name']

            #         #Extract goalkeeper type data
            #         gk_type_id=event_data_entry['goalkeeper']['type']['id']
            #         gk_type_name=event_data_entry['goalkeeper']['type']['name']
            #         #Extract goalkeeper type data
            #         gk_body_part_id=event_data_entry['goalkeeper']['body_part']['id']
            #         gk_body_part_name=event_data_entry['goalkeeper']['body_part']['name']
            #         #Extract goalkeeper outcome data
            #         gk_outcome_id=event_data_entry['goalkeeper']['outcome']['id']
            #         gk_outcome_name=event_data_entry['goalkeeper']['outcome']['name']
            #         #Extract goalkeeper technique data
            #         gk_technique_id=event_data_entry['goalkeeper']['technique']['id']
            #         gk_technique_name=event_data_entry['goalkeeper']['technique']['name']

            #         #Extract clearance data
            #         left_foot=event_data_entry['clearance']['left_foot']
            #         right_foot=event_data_entry['clearance']['right_foot']
            #         head=event_data_entry['clearance']['head']
            #         clearance_aerial_won=event_data_entry['clearance']['aerial_won']

            #         #Extract clearance_body_part data
            #         clearance_body_part_id=event_data_entry['clearance']['body_part']['id']
            #         clearance_body_part_name=event_data_entry['clearance']['body_part']['name']

            #         # Extract duel_type data
            #         duel_type_id=event_data_entry['duel']['type']['id']
            #         duel_type_name=event_data_entry['duel']['type']['name']

            #         # Extract duel_outcome data
            #         duel_outcome_id=event_data_entry['duel']['outcome']['id']
            #         duel_outcome_name=event_data_entry['duel']['outcome']['name']

            #         # Extract ball_receipt_outcome data
            #         ball_receipt_outcome_id=event_data_entry['ball_receipt']['outcome']['id']
            #         ball_receipt_outcome_name=event_data_entry['ball_receipt']['outcome']['name']

            #         # Extract substitution_outcome data
            #         substitution_outcome_id=event_data_entry['substitution']['outcome']['id']
            #         substitution_outcome_name=event_data_entry['substitution']['outcome']['name']

            #         # Extract substitution_replacement data
            #         substitution_replacement_id=event_data_entry['substitution']['replacement']['id']
            #         substitution_replacement_name=event_data_entry['substitution']['replacement']['name']

            #         # Extract data for foul_commited
            #         foul_commited_counterpress=event_data_entry['foul_commited']['counterpress']
            #         foul_commited_offensive=event_data_entry['foul_commited']['offensive']
            #         foul_commited_advantage=event_data_entry['foul_commited']['advantage']
            #         foul_commited_penalty=event_data_entry['foul_commited']['penalty']

            #         # Extract data for foul_commited_type
            #         foul_commited_type_id=event_data_entry['foul_commited']['type']['id']
            #         foul_commited_type_name=event_data_entry['foul_commited']['type']['name']
            #         # Extract data for foul_commited_card
            #         foul_commited_card_id=event_data_entry['foul_commited']['card']['id']
            #         foul_commited_card_name=event_data_entry['foul_commited']['card']['name']

            #         #Extract data for foul_won
            #         foul_won_defensive=event_data_entry['foul_won']['defensive']
            #         foul_won_advantage=event_data_entry['foul_won']['advantage']
            #         foul_won_penalty=event_data_entry['foul_won']['penalty']
                except KeyError as e:

                #     print(f"KeyError: '{e.args[0]}' not found in event data.")
                        continue


                    # Ensure all values are properly assigned, including when the lists are empty

                #     cur.execute("""
                #         INSERT INTO events 
                #         (event_id, index, period, timestamp, minute, second, possession, duration, location, under_pressure, off_camera, out, counterpress, related_events, recovery_failure, carry_end_location, formation, gk_end_location) 
                #         VALUES 
                #         (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                #         ON CONFLICT (event_id) DO NOTHING;
                #     """, (
                #         event_id, index, period, timestamp_obj, minute, second, possession, duration, location_array, under_pressure, off_camera, out, counterpress, related_events_array, recovery_failure, carry_end_location_array, formation, gk_end_location_array
                #     ))

                #     # Insert data into the event_type table
                #     cur.execute("""
                #         INSERT INTO event_type 
                #         (event_id, event_type_id, event_type_name) 
                #         VALUES 
                #         (%s, %s, %s)
                #         ON CONFLICT (event_type_id) DO NOTHING;
                #     """, (
                #         event_id, event_type_id, event_type_name
                # ))
                    # Insert data into the possession_team table
                   
                    # cur.execute("""
                    #     INSERT INTO possession_team 
                    #     (event_id, possession_team_id, possession_team_name) 
                    #     VALUES 
                    #     (%s, %s, %s)
                    #     ON CONFLICT (possession_team_id) DO NOTHING;
                    
                    # """, (
                    #     event_id, possession_team_id, possession_team_name
                    # ))
               
                         
                    # # Insert data into the play_pattern table
                    
                    # cur.execute("""
                    #     INSERT INTO play_pattern 
                    #     (event_id, play_pattern_id, play_pattern_name) 
                    #     VALUES 
                    #     (%s, %s, %s)
                    #     ON CONFLICT (play_pattern_id) DO NOTHING;
                        
                    # """, (
                    #     event_id, play_pattern_id, play_pattern_name
                    # ))
                    
                # # Insert data into the team table
                #     cur.execute("""
                #         INSERT INTO team 
                #         (event_id, team_id, team_name) 
                #         VALUES 
                #         (%s, %s, %s)
                #         ON CONFLICT (team_id) DO NOTHING;
                #     """, (
                #         event_id, team_id, team_name
                #     ))
                #     # Insert data into the event_player table
                #     cur.execute("""
                #         INSERT INTO event_player 
                #         (event_id, event_player_id, event_player_name) 
                #         VALUES 
                #         (%s, %s, %s)
                #         ON CONFLICT (event_player_id) DO NOTHING;
                #     """, (
                #         event_id, event_player_id, event_player_name
                #     ))
                # Insert data into the event_position table
            #     cur.execute("""
            #         INSERT INTO event_position 
            #         (event_id, event_position_id, event_position_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (event_position_id) DO NOTHING;
            #     """, (
            #     event_id, event_position_id, event_position_name
            # ))
            
                # # Insert data into the pass table
                # cur.execute("""
                #     INSERT INTO pass 
                #     (event_id, length, angle, pass_end_location, "switch", "cross", assisted_shot_id, backheel, deflected, miscommunication, "cut-back", shot_assist, "goal-assist", pass_aerial_won) 
                #     VALUES 
                #     (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                # """, (
                #     event_id, length, angle, pass_end_location_array, switch, cross, assisted_shot_id, backheel, deflected, miscommunication, cutback, shot_assist, goalassist, pass_aerial_won
                # ))
                # # Insert data into the recipient table
                # cur.execute("""
                #     INSERT INTO recipient 
                #     (event_id, recipient_id, recipient_name) 
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (recipient_id) DO NOTHING;
                # """, (
                #     event_id, recipient_id, recipient_name
                # ))
                # # Insert data into the height table
                # cur.execute("""
                #     INSERT INTO height 
                #     (event_id, height_id, height_name) 
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (height_id) DO NOTHING;
                # """, (
                #     event_id, height_id, height_name
                # ))
                # # Insert data into the pass_body_part table
                # cur.execute("""
                #     INSERT INTO pass_body_part 
                #     (event_id, pass_body_part_id, pass_body_part_name) 
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (pass_body_part_id) DO NOTHING;
                # """, (
                #     event_id, pass_body_part_id, pass_body_part_name
                # ))
                # # Insert data into the pass_type table
                # cur.execute("""
                #     INSERT INTO pass_type 
                #     (event_id, pass_type_id, pass_type_name) 
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (pass_type_id) DO NOTHING;
                # """, (
                #     event_id, pass_type_id, pass_type_name
                # ))
                # # Insert data into the pass_outcome table
                # cur.execute("""
                #     INSERT INTO pass_outcome 
                #     (event_id, pass_outcome_id, pass_outcome_name) 
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (pass_outcome_id) DO NOTHING;
                # """, (
                #     event_id, pass_outcome_id, pass_outcome_name
                # ))
                # # Insert data into the pass_technique table
                # cur.execute("""
                #     INSERT INTO pass_technique 
                #     (event_id, pass_technique_id, pass_technique_name)
                     
                #     VALUES 
                #     (%s, %s, %s)
                #     ON CONFLICT (pass_technique_id) DO NOTHING;
                # """, (
                #     event_id, pass_technique_id, pass_technique_name
                # ))
                #Insert data into the shot table
            
                # Insert data into the shot_body_part table
                cur.execute("""
                    INSERT INTO shot_body_part 
                    (event_id, shot_body_part_id, shot_body_part_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_body_part_id) DO NOTHING; 
                """, (
                    event_id, shot_body_part_id, shot_body_part_name
                ))
                # Insert data into the shot_type table
                cur.execute("""
                    INSERT INTO shot_type 
                    (event_id, shot_type_id, shot_type_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_type_id) DO NOTHING;
                """, (
                    event_id, shot_type_id, shot_type_name
                ))
                # Insert data into the shot_outcome table
                cur.execute("""
                    INSERT INTO shot_outcome 
                    (event_id, shot_outcome_id, shot_outcome_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_outcome_id) DO NOTHING;
                """, (
                    event_id, shot_outcome_id, shot_outcome_name
                ))
                # Insert data into the shot_technique table
                cur.execute("""
                    INSERT INTO shot_technique 
                    (event_id, shot_technique_id, shot_technique_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_technique_id) DO NOTHING;
                """, (
                    event_id, shot_technique_id, shot_technique_name
                ))
                # Insert data into the shot_freeze_frame_player table
                cur.execute("""
                    INSERT INTO shot_freeze_frame_player 
                    (event_id, shot_freeze_frame_player_id, shot_freeze_frame_player_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_freeze_frame_player_id) DO NOTHING;
                """, (
                    event_id, shot_freeze_frame_player_id, shot_freeze_frame_player_name
                ))
                  # Insert data into the shot_freeze_frame_position table
                cur.execute("""
                    INSERT INTO shot_freeze_frame_position
                    (event_id, shot_freeze_frame_position_id, shot_freeze_frame_position_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (shot_freeze_frame_position_id) DO NOTHING;
                """, (
                    event_id, shot_freeze_frame_position_id, shot_freeze_frame_position_name
                ))
                # Insert data into the gk_position table
                cur.execute("""
                    INSERT INTO gk_position 
                    (event_id, gk_position_id, gk_position_name) 
                    VALUES 
                    (%s, %s, %s)
                    ON CONFLICT (gk_position_id) DO NOTHING;
                """, (
                    event_id, gk_position_id, gk_position_name
                ))
            #     # Insert data into the gk_type table
            #     cur.execute("""
            #         INSERT INTO gk_type 
            #         (event_id, gk_type_id, gk_type_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (gk_type_id) DO NOTHING;
            #     """, (
            #         event_id, gk_type_id, gk_type_name
            #     ))
            #     # Insert data into the gk_body_part table
            #     cur.execute("""
            #         INSERT INTO gk_body_part 
            #         (event_id, gk_body_part_id, gk_body_part_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (gk_body_part_id) DO NOTHING;
            #     """, (
            #         event_id, gk_body_part_id, gk_body_part_name
            #     ))
            #     # Insert data into the gk_outcome table
            #     cur.execute("""
            #         INSERT INTO goalkeeper_outcome 
            #         (event_id, gk_outcome_id, gk_outcome_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (gk_outcome_id) DO NOTHING;
            #     """, (
            #         event_id, gk_outcome_id, gk_outcome_name
            #     ))
            #     # Insert data into the goalkeeper_technique table
            #     cur.execute("""
            #         INSERT INTO goalkeeper_technique 
            #         (event_id, gk_technique_id, gk_technique_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (gk_technique_id) DO NOTHING;
            #     """, (
            #         event_id, gk_technique_id, gk_technique_name
            #     ))
            #     # Insert data into the clearance table
            #     cur.execute("""
            #         INSERT INTO clearance 
            #         (event_id, left_foot, right_foot, head, aerial_won) 
            #         VALUES 
            #         (%s, %s, %s, %s, %s)
            #     """, (
            #         event_id, left_foot, right_foot, head, clearance_aerial_won
            #     ))
            #     # Insert data into the clearance_body_part table
            #     cur.execute("""
            #         INSERT INTO clearance_body_part 
            #         (event_id, clearance_body_part_id, clearance_body_part_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (clearance_body_part_id) DO NOTHING;
            #     """, (
            #         event_id, clearance_body_part_id, clearance_body_part_name
            #     ))
            #     # Insert data into the duel_type table
            #     cur.execute("""
            #         INSERT INTO duel_type 
            #         (event_id, duel_type_id, duel_type_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (duel_type_id) DO NOTHING;
            #     """, (
            #         event_id, duel_type_id, duel_type_name
            #     ))
            #     # Insert data into the duel_outcome table
            #     cur.execute("""
            #         INSERT INTO duel_outcome 
            #         (event_id, duel_outcome_id, duel_outcome_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (duel_outcome_id) DO NOTHING;
            #     """, (
            #         event_id, duel_outcome_id, duel_outcome_name
            #     ))
            #     # Insert data into the ball_receipt_outcome table
            #     cur.execute("""
            #         INSERT INTO ball_receipt_outcome 
            #         (event_id, ball_receipt_outcome_id, ball_receipt_outcome_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (ball_receipt_outcome_id) DO NOTHING;
            #     """, (
            #         event_id, ball_receipt_outcome_id, ball_receipt_outcome_name
            #     ))
            #     # Insert data into the substitution_outcome table
            #     cur.execute("""
            #         INSERT INTO substitution_outcome 
            #         (event_id, substitution_outcome_id, substitution_outcome_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (substitution_outcome_id) DO NOTHING;
            #     """, (
            #         event_id, substitution_outcome_id, substitution_outcome_name
            #     ))
            #     # Insert data into the foul_commited table
            #     cur.execute("""
            #         INSERT INTO foul_commited 
            #         (event_id, counterpress, offensive, advantage, penalty) 
            #         VALUES 
            #         (%s, %s, %s, %s, %s)
            #     """, (
            #         event_id, foul_commited_counterpress, foul_commited_offensive, foul_commited_advantage, foul_commited_penalty
            #     ))
            #     # Insert data into the foul_commited_type table
            #     cur.execute("""
            #         INSERT INTO foul_commited_type 
            #         (event_id, foul_commited_type_id, foul_commited_type_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (foul_commited_type_id) DO NOTHING;
            #     """, (
            #         event_id, foul_commited_type_id, foul_commited_type_name
            #     ))
            #     # Insert data into the foul_commited_card table
            #     cur.execute("""
            #         INSERT INTO foul_commited_card 
            #         (event_id, foul_commited_card_id, foul_commited_card_name) 
            #         VALUES 
            #         (%s, %s, %s)
            #         ON CONFLICT (foul_commited_card_id) DO NOTHING;
            #     """, (
            #         event_id, foul_commited_card_id, foul_commited_card_name
            #     ))
            #     cur.execute("""
            #         INSERT INTO foul_won 
            #         (event_id, defensive, advantage, penalty) 
            #         VALUES 
            #         (%s, %s, %s, %s)
            #     """, (
            #         event_id, foul_won_defensive, foul_won_advantage, foul_won_penalty
            #     ))

                    



# Commit changes to the database
conn.commit()

#Insert values into lineups tables
for match_id in match_id_set:
        #print('in first for-loop')
        lineups_file_path = os.path.join(data_dir, 'lineups', f"{match_id}.json")
        lineups_data = read_json_file(events_file_path)
        for lineup_data_entry in lineups_data: 
            #Extract data from lineup_data_entry
            team_id = lineup_data_entry['team_id']
            team_name = lineup_data_entry['team_name']
            player_id = lineup_data_entry['player_id']
            player_name = lineup_data_entry['player_name']
            player_nickname = lineup_data_entry.get('player_nickname', None)
            jersey_number = lineup_data_entry.get('jersey_number', None)
            player_country_id = lineup_data_entry.get('player_country_id', None)
            player_country_name = lineup_data_entry.get('player_country_name', None)
            position_id = lineup_data_entry.get('position_id', None)
            position_name = lineup_data_entry.get('position_name', None)
            start_time = lineup_data_entry.get('start_time', None)
            end_time = lineup_data_entry.get('end_time', None)
            start_period = lineup_data_entry.get('start_period', None)
            end_period = lineup_data_entry.get('end_period', None)
            start_reason = lineup_data_entry.get('start_reason', None)
            end_reason = lineup_data_entry.get('end_reason', None)

            # Insert data into the team_lineup table
            cur.execute("""
                INSERT INTO team_lineup 
                (team_id, team_name, player_id, player_name, player_nickname, jersey_number, 
                player_country_id, player_country_name, position_id, position_name, 
                start_time, end_time, start_period, end_period, start_reason, end_reason) 
                VALUES 
                (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT(team_id) DO NOTHING;
            """, (
                team_id, team_name, player_id, player_name, player_nickname, jersey_number,
                player_country_id, player_country_name, position_id, position_name,
                start_time, end_time, start_period, end_period, start_reason, end_reason
            ))
            # Extract data from lineup_data_entry
            player_id = lineup_data_entry['player_id']
            player_name = lineup_data_entry['player_name']
            player_nickname = lineup_data_entry.get('player_nickname', None)
            jersey_number = lineup_data_entry.get('jersey_number', None)

            # Insert data into the lineup table
            cur.execute("""
                INSERT INTO lineup (team_id,player_id, player_name, player_nickname, jersey_number)
                VALUES (%s,%s, %s, %s, %s)
            """, (
                team_id,player_id, player_name, player_nickname, jersey_number
            ))
            # Extract data from lineup_data_entry
            lineup_country_id = lineup_data_entry['country']['id']
            lineup_country_name = lineup_data_entry['country']['name']

            # Insert data into the lineup_country table
            cur.execute("""
                INSERT INTO lineup_country (team_id, lineup_country_id, lineup_country_name)
                VALUES (%s, %s, %s)
                ON CONFLICT(lineup_country_id) DO NOTHING;
            """, (
                team_id, lineup_country_id, lineup_country_name
            ))
            # Extract data from lineup_data_entry
            time = lineup_data_entry.get('time', None)
            card_type = lineup_data_entry.get('card_type', None)
            reason = lineup_data_entry.get('reason', None)
            period = lineup_data_entry.get('period', None)

            # Insert data into the lineup_cards table
            cur.execute("""
                INSERT INTO lineup_cards (team_id, time, card_type, reason, period)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                team_id, time, card_type, reason, period
            ))
            position_id = lineup_data_entry.get('position_id', None)
            position = lineup_data_entry.get('position', None)
            from_timestamp = lineup_data_entry.get('from', None)
            to_timestamp = lineup_data_entry.get('to', None)
            from_period = lineup_data_entry.get('from_period', None)
            to_period = lineup_data_entry.get('to_period', None)
            start_reason = lineup_data_entry.get('start_reason', None)
            end_reason = lineup_data_entry.get('end_reason', None)

            # Insert data into the lineup_positions table
            cur.execute("""
                INSERT INTO lineup_positions 
                (team_id, position_id, position, "from", "to", from_period, to_period, start_reason, end_reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                team_id, position_id, position, from_timestamp, to_timestamp, 
                from_period, to_period, start_reason, end_reason
            ))




targeted_competitions = ['La Liga', 'Premier League']
targeted_seasons = ['2018/2019', '2019/2020', '2020/2021', '2003/2004']
# Insert data from lineups folder for La Liga 2018-2019, 2019-2020, 2020-2021 and Premier League 2003-2004
for competition_id in targeted_competitions:
    for season_id in targeted_seasons:
        lineups_folder = os.path.join(data_dir, 'lineups', f'{competition_id}')
        if os.path.exists(lineups_folder):
            season_lineups_folder = os.path.join(lineups_folder, f'{season_id}.json')
            if os.path.exists(season_lineups_folder):
                lineups_data = read_json_file(season_lineups_folder)
                for lineup in lineups_data:
                    # Insert lineup data into the database
                    # Adjust column names and data according to your database schema
                    lineup_id = lineup['id']
                    lineup_json = json.dumps(lineup)
                    cur.execute("""
                        INSERT INTO lineups (lineup_id, lineup_json)
                        VALUES (%s, %s)
                        ON CONFLICT (lineup_id) DO NOTHING;
                    """, (lineup_id, lineup_json))

# Commit changes to the database
conn.commit()

# Close cursor and connection
cur.close()
conn.close()
