
import boto3 
import requests 
import pandas as pd 
from unidecode import unidecode 


# function to get the players 
def get_players(aws_client, bucket_name): 

    # call the API to get the players 
    url = f"https://api.sleeper.app/v1/players/nba" 
    response = requests.get(url)  
    players = response.json() 

    # function to convert to integer 
    def convert_int(x): 
        try: 
            return int(x) 
        except: 
            return -1 

    # loop through the players and add to the dataframe 
    df = pd.DataFrame() 
    for k, val in players.items(): 

        # get the player positions 
        positions = val.get('fantasy_positions', []) 
        if positions is None: 
            positions = [] 
        
        df = pd.concat([df, pd.DataFrame({
            "PLAYER_ID_SLEEPER": [convert_int(k)],  
            "FIRST_NAME": val.get('first_name', None), 
            "LAST_NAME": val.get('last_name', None), 
            "TEAM_ABBR": val.get('team', None), 
            "POSITIONS": ", ".join(positions), 
            "TEAM_STATUS": val.get('status', None), 
            "SEARCH_RANK": val.get('search_rank', None), 
            "DC_POSITION": val.get('depth_chart_position', None) 
        })], ignore_index = True) 

    # remove accents 
    df["FIRST_NAME"] = df["FIRST_NAME"].apply(lambda x: unidecode(str(x))) 
    df["LAST_NAME"] = df["LAST_NAME"].apply(lambda x: unidecode(str(x))) 

    # concatenate player names 
    df["PLAYER_NAME"] = df["FIRST_NAME"] + " " + df["LAST_NAME"] 

    # showcase the total number of players 
    nplayers = df.shape[0] 
    print(f"Total players: {nplayers:,}") 

    # save the dataframe to S3 
    csv_string = df.to_csv(index = False) 
    aws_client.put_object(
        Bucket = bucket_name, 
        Key = "sleeper_players.csv", 
        Body = csv_string
    ) 

    return df 

# function to get the league users 
def get_league_users(aws_client, league_id, bucket_name): 

    # get the users 
    url = f"https://api.sleeper.app/v1/league/{league_id}/users" 
    users = requests.get(url).json() 

    # loop through the users and add to a dataframe 
    df = pd.DataFrame() 
    for i, user in enumerate(users):
        df = pd.concat([
            df, 
            pd.DataFrame({
                "USER_ID": [user['user_id']], 
                "NAME": [user['display_name']] 
            })
        ]) 
    
    # save the dataframe to S3 
    csv_string = df.to_csv(index = False) 
    aws_client.put_object(
        Bucket = bucket_name, 
        Key = "sleeper_users.csv", 
        Body = csv_string
    ) 

    return df 

# function to get the league rosters 
def get_league_rosters(aws_client, league_id, bucket_name): 

    # get the rosters 
    url = f"https://api.sleeper.app/v1/league/{league_id}/rosters" 
    rosters = requests.get(url).json() 

    # loop through the rosters and add to a dataframe 
    df = pd.DataFrame() 
    for i, roster in enumerate(rosters):

        # get the players 
        players = roster['players'] 
        starters = roster['starters'] 
        is_starter = [1 if p in starters else 0 for p in players] 

        # add to the dataframe 
        df = pd.concat([
            df, 
            pd.DataFrame({
                "ROSTER_ID": roster.get("roster_id", None),
                "OWNER_ID": roster.get("owner_id", None), 
                "PLAYER_ID_SLEEPER": roster["players"], 
                "IS_STARTER": is_starter 
            })
        ], ignore_index = True) 
    
    # save the dataframe to S3 
    csv_string = df.to_csv(index = False) 
    aws_client.put_object(
        Bucket = bucket_name, 
        Key = "sleeper_rosters.csv", 
        Body = csv_string
    ) 

    return df 

# handler function 
def lambda_handler(event, context):

    # hard coded parameters 
    bucket_name = "fantasy-basketball-data" 
    league_id = 1150564175471755264  

    # create the clients that we need  
    aws_client = boto3.client('s3') 

    # get the sleeper league data 
    df_players = get_players(aws_client, bucket_name) 
    df_users = get_league_users(aws_client, league_id, bucket_name) 
    df_rosters = get_league_rosters(aws_client, league_id, bucket_name) 

    print(df_players.head()) 
    print(df_users.head()) 
    print(df_rosters.head())  