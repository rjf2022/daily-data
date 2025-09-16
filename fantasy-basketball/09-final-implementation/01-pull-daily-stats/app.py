
import boto3 
import json 
import time 
import requests 
import numpy as np 
import pandas as pd 

# function to call the get endpoint with error handling 
def call_get_endpoint(endpoint, params, headers): 

    # build the url 
    url = f"https://api-nba-v1.p.rapidapi.com/{endpoint}" 

    # make the request 
    response = requests.get(url, headers = headers, params = params) 

    # get the data if the request was successful 
    if response.status_code == 200: 
        data = response.json() 
    
    # otherwise, prompt the user with the error message and return None 
    else: 
        print(f"Error: {response.status_code}") 
        data = None 
    
    return data 

# function to calculate the week number based on the date 
def calculate_week_number(dt, first_date):

    # calucate the number of days between the two dates 
    days = (dt - first_date).days 

    # calculate the week number 
    week_number = (days // 7) + 1 

    return week_number 

# function to get and save the games per date 
def get_all_games(aws_client, headers):

    # get all of the games on the given date 
    params = {
        "season": "2024" 
    } 

    # call the API 
    data = call_get_endpoint("games", params, headers) 

    # transform the data into a dataframe 
    df_games = pd.DataFrame() 
    for game in data["response"]: 
        df_games = pd.concat([
            df_games, 
            pd.DataFrame({
                "GAME_ID": [game["id"]], 
                "LEAGUE": game["league"], 
                "GAME_DATE": game["date"]["start"], 
                "HOME_ID": game["teams"]["home"]["id"], 
                "HOME_TEAM": game["teams"]["home"]["name"], 
                "GUEST_ID": game["teams"]["visitors"]["id"], 
                "GUEST_TEAM": game["teams"]["visitors"]["name"] 
            }) 
        ]) 

    # reset the index 
    df_games.reset_index(drop = True, inplace = True) 

    # convert to Mountain time 
    df_games["GAME_DATE"] = pd.to_datetime(df_games["GAME_DATE"]).dt.tz_convert("US/Mountain").dt.tz_localize(None) 

    # first day of the first week of the season 
    first_date = pd.to_datetime("2024-10-21") 

    # filter to only regular season games 
    df_games = df_games.loc[df_games["GAME_DATE"] >= first_date] 

    # calculate the week number 
    df_games["WEEK_NUMBER"] = df_games["GAME_DATE"].apply(lambda x: calculate_week_number(x, first_date)) 

    # convert the dataframe to a csv string 
    csv_string = df_games.to_csv(index = False) 

    # write to the bucket 
    aws_client.put_object(
        Body = csv_string,
        Bucket = "fantasy-basketball-data", 
        Key = "all_games.csv" 
    ) 

    return df_games 

# function to get and save the stats for all games on a given date 
def get_daily_stats(game_date, df_games, aws_client, headers):

    # get the dataframe of daily games 
    print(f"Getting games for {game_date}...") 
    df_games = df_games.loc[df_games["GAME_DATE"].dt.strftime("%Y-%m-%d") == game_date].reset_index(drop = True)  
    
    # sleep for a bit to avoid rate limiting 
    time.sleep(8) 

    # loop through each game and get the stats 
    df_stats = pd.DataFrame() 
    for i, row in df_games.iterrows(): 
        print(f"Getting stats for game {i + 1} out of {len(df_games.index)}") 

        # get the game stats 
        params = {
            'game': row["GAME_ID"]
        } 

        # call the API 
        data = call_get_endpoint("players/statistics", params, headers) 

        # loop through and add the stats to the dataframe 
        for stats in data["response"]:
            df_stats = pd.concat([ 
                df_stats, 
                pd.DataFrame({
                    "GAME_ID": [row["GAME_ID"]], 
                    "PLAYER_ID": stats["player"]["id"], 
                    "TEAM_ID": stats["team"]["id"], 
                    "POINTS": stats["points"], 
                    "POS": stats["pos"], 
                    "MIN": stats["min"], 
                    "FGM": stats["fgm"], 
                    "FGA": stats["fga"], 
                    "FGP": stats["fgp"], 
                    "FTM": stats["ftm"], 
                    "FTA": stats["fta"], 
                    "FTP": stats["ftp"], 
                    "TPM": stats["tpm"], 
                    "TPA": stats["tpa"], 
                    "TPP": stats["tpp"], 
                    "OFFREB": stats["offReb"], 
                    "DEFREB": stats["defReb"], 
                    "TOTREB": stats["totReb"], 
                    "ASSISTS": stats["assists"], 
                    "PFOULS": stats["pFouls"], 
                    "STEALS": stats["steals"], 
                    "TURNOVERS": stats["turnovers"], 
                    "BLOCKS": stats["blocks"], 
                    "PLUSMINUS": stats["plusMinus"] 
                }) 
            ]) 
        
        # sleep for a bit to avoid rate limiting 
        time.sleep(8) 

    # reset the index 
    df_stats.reset_index(drop = True, inplace = True) 

    # convert the dataframe to a csv string 
    csv_string = df_stats.to_csv(index = False) 

    # write to the bucket 
    aws_client.put_object(
        Body = csv_string,
        Bucket = "fantasy-basketball-data", 
        Key = f"Stats/stats {game_date}.csv" 
    ) 

    return df_stats 

# function to pull the dates that we need data for 
def pull_missing_dates(aws_client, bucket_name): 

    # first day of the season 
    league_start = "2024-10-22" 

    # get yesterday's date 
    yesterday = pd.Timestamp.now(tz = "US/Mountain").tz_localize(None).normalize() - pd.Timedelta(days = 1) 

    # dates between the start and end date 
    all_dates = pd.date_range(league_start, yesterday, freq = "D").strftime("%Y-%m-%d")

    # List all files in the specified folder
    response = aws_client.list_objects_v2(
        Bucket = bucket_name, 
        Prefix = "Stats/"
    ) 

    # get all the dates into a list
    dates_processed = [] 
    for obj in response["Contents"]: 
        fname = obj["Key"] 

        # make sure that the file is a CSV file before trying to add 
        if ".csv" in fname: 
            dates_processed.append(fname.split(" ")[1].split(".")[0]) 

    # filter out the dates that have already been processed 
    pull_dates = [date for date in all_dates if date not in dates_processed] 

    return pull_dates 

# function to calculate fantasy points based on stats 
def calculate_fantasy_points(df):

    # calculate the double double and triple double flags 
    double_columns = ['POINTS', 'TOTREB', 'ASSISTS', 'STEALS', 'BLOCKS']
    df['DD'] = df[double_columns].apply(lambda row: (row >= 10).sum() >= 2, axis=1)
    df['TD'] = df[double_columns].apply(lambda row: (row >= 10).sum() >= 3, axis=1) 

    # miss calculations 
    df["FGMI"] = df["FGA"] - df["FGM"] 
    df["FTMI"] = df["FTA"] - df["FTM"] 
    df["TPMI"] = df["TPA"] - df["TPM"] 

    # threshold columns 
    df["PB40"] = np.where(df["POINTS"] >= 40, 1, 0) 
    df["PB50"] = np.where(df["POINTS"] >= 50, 1, 0) 
    df["AB15"] = np.where(df["ASSISTS"] >= 15, 1, 0) 
    df["RB20"] = np.where(df["TOTREB"] >= 20, 1, 0) 

    # calculate the fantasy points 
    df["FANTASY_POINTS"] = (
        (df["POINTS"] * 1) + # +1 for each point scored 
        (df["TOTREB"] * 1) + # +1 for each rebound 
        (df["ASSISTS"] * 1) + # +1 for each assist 
        (df["STEALS"] * 1.5) + # +1.5 for each steal 
        (df["BLOCKS"] * 1.5) + # +1.5 for each block 
        (df["TURNOVERS"] * -1) + # -1 for each turnover 
        (df["DD"] * 5) + # +5 for double doubles 
        (df["TD"] * 10) + # +10 for triple doubles 
        (df["FGM"] * 0.5) + # +0.5 for field goals made 
        ((df["FGMI"]) * -0.5) + # -0.5 for two or three point misses 
        (df["FTM"] * 1) + # +1 for free throws made 
        (df["FTMI"] * -1) + # -1 for free throw misses 
        (df["TPM"] * 2) + # +2 for three pointers made 
        (df["TPMI"] * -1) + # -1 for three point misses 
        (df["OFFREB"] * 1.5) + # +1.5 for offensive rebounds 
        (df["DEFREB"] * 1) + # +1 for defensive rebounds 
        (df["PB40"] * 2) + # +2 for scoring 40+ points 
        (df["PB50"] * 3) + # +3 for scoring 50+ points 
        (df["AB15"] * 2) + # +2 for 15+ assists 
        (df["RB20"] * 2) # +2 for 20+ rebounds 
    ) 

    return df 

# function to read one of the CSV files from S3 into a pandas dataframe 
def read_s3_csv(csv_file, aws_client, bucket_name): 

    # read the file from S3 
    obj = aws_client.get_object(Bucket = bucket_name, Key = f"{csv_file}.csv") 

    # read the file into a pandas dataframe 
    df = pd.read_csv(obj['Body']) 

    # change any id columns to integers if need be 
    int_cols = ["GAME_ID"] 
    for col in int_cols: 
        if col in df.columns: 
            df[col] = df[col].fillna(0).astype(int) 

    return df 

# function to combine all of the daily stats files into one big file 
def combine_all_stats(aws_client, bucket_name): 
    
    # get the game data 
    df_games = read_s3_csv("all_games", aws_client, bucket_name) 

    # get all of the files in the bucket 
    files = aws_client.list_objects(Bucket = bucket_name)["Contents"] 

    # loop through the files and add to one big dataframe 
    df = pd.DataFrame() 
    for i, file in enumerate(files): 

        # get the file name and make sure it is a csv file 
        fname = file["Key"] 
        if ("Stats/stats" in fname) and (".csv" in fname): 
            try:

                # get the object and read the data 
                obj = aws_client.get_object(Bucket = bucket_name, Key = fname) 
                df_new = pd.read_csv(obj["Body"]) 

                # append the data to the big dataframe 
                df = pd.concat([df, df_new]) 
            
            except:
                pass # (no data in the file) 
    
    # join in the games data 
    df = df.merge(df_games[["GAME_ID", "GAME_DATE", "WEEK_NUMBER"]], on = "GAME_ID", how = "left") 

    # calculate the fantasy points 
    df = calculate_fantasy_points(df) 

    # convert the dataframe to a csv string 
    csv_string = df.to_csv(index = False) 

    # write to the bucket 
    aws_client.put_object(
        Body = csv_string,
        Bucket = "fantasy-basketball-data", 
        Key = f"all_stats.csv" 
    ) 
    
    return df 


# handler function 
def lambda_handler(event, context):

    # AWS file locations 
    bucket_name = "fantasy-basketball-data" 

    # create the clients that we need  
    aws_client = boto3.client('s3') 
    secrets_client = boto3.client('secretsmanager') 

    # get the API key 
    secret = secrets_client.get_secret_value(SecretId = "fantasyBasketball/APIKey") 
    api_key = json.loads(secret["SecretString"])["NBA_API_KEY"] 

    # setup the authorization headers 
    headers = { 'x-rapidapi-key': api_key } 

    # get all of the games 
    df_games = get_all_games(aws_client, headers) 

    # figure out the dates that we need to pull today 
    pull_dates = pull_missing_dates(aws_client, bucket_name)  

    # loop through each of the dates and pull the stats 
    for game_date in pull_dates: 

        # get the stats on a given day 
        df_stats = get_daily_stats(
            game_date = game_date, 
            df_games = df_games, 
            aws_client = aws_client,  
            headers = headers 
        ) 
    
    # combine all the stats together 
    combine_all_stats(aws_client, bucket_name) 
