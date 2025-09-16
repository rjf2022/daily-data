
import boto3 
import json 
import smtplib 
import ssl 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import pandas as pd 
import numpy as np 
from scipy.stats import norm 



# function to read one of the CSV files from S3 into a pandas dataframe 
def read_s3_csv(csv_file, aws_client, bucket_name): 

    # read the file from S3 
    obj = aws_client.get_object(Bucket = bucket_name, Key = f"{csv_file}.csv") 

    # read the file into a pandas dataframe 
    df = pd.read_csv(obj['Body']) 

    # change any columns to integers if need be 
    int_cols = ["GAME_ID", "WEEK_NUMBER", "PLAYER_ID"] 
    for cname in df.columns:
        if cname in int_cols: 
            df[cname] = df[cname].fillna(0).astype(int) 
    
    # change any date/time columns to datetime if need be 
    date_cols = ["GAME_DATE"] 
    for cname in df.columns:
        if cname in date_cols: 
            df[cname] = pd.to_datetime(df[cname]) 

    return df 


# probability that there will be a better score given the parameters and number of games left 
def calc_prob_better(current, games_left, mean, sd):

    prob_single = norm.cdf(current, loc = mean, scale = sd) 
    prob_any = 1 - (prob_single ** games_left) 

    return prob_any 


def simulate_locked_points(df_players, nreps = 100):

    # expand by the game number 
    df = df_players.copy() 
    df["GAME_NUM"] = df["GAMES"].apply(lambda x: np.arange(1, x + 1)) 
    df = df.explode("GAME_NUM") 

    # expand by the simulation repetition 
    df = df.reset_index(drop = True) 
    df["REP_NUM"] = df.apply(lambda x: np.arange(1, nreps + 1), axis = 1) 
    df = df.explode("REP_NUM") 

    # simulate the points values 
    df["SIM_POINTS"] = df.apply(lambda x: np.random.normal(loc = x["PTS_MEAN"], scale = x["PTS_SD"]), axis = 1) 

    # calculate the number of games left 
    df["GAMES_LEFT"] = df["GAMES"] - df["GAME_NUM"] 

    # compute which games we will lock in 
    df["PROB_BETTER"] = df.apply(lambda x: calc_prob_better(
        current = x["SIM_POINTS"], 
        games_left = x["GAMES_LEFT"], 
        mean = x["PTS_MEAN"], 
        sd = x["PTS_SD"] 
    ), axis = 1) 
    df = df.loc[df["PROB_BETTER"] < 0.5]  
    df = df.sort_values(by = ["REP_NUM", "GAME_NUM"]).groupby(["PLAYER_ID", "REP_NUM"]).head(1) 

    return df 


def compute_adjusted_projections(df_players):

    # simulate the locked points 
    df_sims = simulate_locked_points(df_players) 

    # calculate the adjusted projections 
    df_adjusted = df_sims.groupby(list(df_players.columns) ).agg({
        "SIM_POINTS": "mean"
    }).reset_index() 

    return df_adjusted 


# # function to create the lock-in report message for the given user 
# def create_lock_report(user_name, df_options):

#     # filter the options for the given user 
#     df_user = df_options.loc[df_options["TEAM_NAME"] == user_name] 
#     df_user = df_user.loc[df_user["IS_STARTER"] == 1] 

#     # sort the options by the probability of scoring above the threshold 
#     df_user = df_user.sort_values("PROB_BETTER", ascending = True).reset_index(drop = True) 

#     # message to add to 
#     message = "Current lock-in report: " 

#     # loop through the options 
#     for i, row in df_user.iterrows(): 
#         message += f"\n\n {row['PLAYER_NAME']}: {row['FANTASY_POINTS']:.1f} current points " 
#         message += f"\n - {row['PROB_BETTER']:.1%} chance of scoring better " 
#         message += f"\n - {row['GAMES_LEFT']:.0f} games left (avg {row['AVG_POINTS']:.1f} points per game) " 

#     return message 


# function to send a series of email messages  
def send_emails(user_name, to_email, email_body, secrets_client): 
    

    # define the users to send emails to 
    mdict = [
        {
            "user_name": user_name, 
            "email": to_email, 
            "message": email_body 
        }
    ] 

    # connection constants  
    smtp_server = "smtp.gmail.com"
    port = 465  # For SSL
    sender_email = "fantasybasketballdata2024@gmail.com"
    
    # get the password from the secrets manager 
    secret = secrets_client.get_secret_value(SecretId = "emailAppPassword") 
    password = json.loads(secret["SecretString"])["email_password"] 

    # setup the email connection 
    context = ssl.create_default_context()
    server = smtplib.SMTP_SSL(smtp_server, port, context = context) 
    server.login(sender_email, password) 

    # get the current date 
    current_date = pd.Timestamp.now(tz = "US/Mountain").tz_localize(None) 

    # loop through the messages and send them 
    for report in mdict: 

        # create the message and add the details
        msg = MIMEMultipart() 
        msg['From'] = sender_email 
        msg['To'] = report["email"] 
        msg['Subject'] = f"Fantasy Basketball Lock-in Report - {current_date:%Y-%m-%d}" 

        # add the message to the email 
        msg.attach(MIMEText(report["message"], 'plain')) 

        # send the email 
        server.sendmail(sender_email, report["email"], msg.as_string()) 

    # close out the connection when we're done 
    server.quit() 


# handler function 
def lambda_handler(event, context):

    # turn off the pd chained assignment warning 
    pd.options.mode.chained_assignment = None 

    # hard coded parameters 
    bucket_name = "fantasy-basketball-data" 
    my_roster = 8 
    user_name = "rjf2023" 
    to_email = "rjfisch07@gmail.com" 

    # create the clients that we need  
    aws_client = boto3.client('s3') 
    secrets_client = boto3.client('secretsmanager') 

    # string for the email body 
    email_body = ""


    ##### current week ##### 

    # first day of the season 
    league_start = pd.to_datetime("2024-10-22") 

    # get the current week number 
    first_date = pd.to_datetime("2024-10-22")
    days_in = (pd.Timestamp.now() - first_date).days 
    week_number = (days_in // 7) + 1 


    ##### summary stats ##### 

    # read in the stats data 
    df_stats = read_s3_csv("all_stats", aws_client, bucket_name) 

    # make sure that the game date is formatted correctly 
    df_stats["GAME_DATE"] = pd.to_datetime(df_stats["GAME_DATE"]) 


    ##### players data  ##### 

    # read in the players data 
    df_players = read_s3_csv("raw_players", aws_client, bucket_name)  

    # put together a full player name column 
    df_players["FULL_NAME"] = (df_players["FIRST_NAME"] + " " + df_players["LAST_NAME"]).str.upper() 

    # read in the Sleeper players and rosters 
    df_splayers = read_s3_csv("sleeper_players", aws_client, bucket_name) 
    df_rosters = read_s3_csv("sleeper_rosters", aws_client, bucket_name) 

    # uppercase the names to match the other data 
    df_splayers["FULL_NAME"] = df_splayers["PLAYER_NAME"].str.upper() 

    # merge the rosters into the players 
    df_splayers = (
        df_splayers.merge(df_rosters, how = "left", on = "PLAYER_ID_SLEEPER") 
        [["FULL_NAME", "ROSTER_ID", "IS_STARTER", "SEARCH_RANK"]]
    ) 

    # join to the other players dataframe 
    df_players = df_splayers.merge(df_players, how = "left", on = "FULL_NAME") 
    df_players = df_players.loc[~df_players["PLAYER_ID"].isnull()] 

    # aggregate the stats data 
    agg_stats = df_stats.groupby("PLAYER_ID").agg(
        PTS_MEAN = ("FANTASY_POINTS", "mean"), 
        PTS_SD = ("FANTASY_POINTS", "std") 
    ) 

    # join in the aggregated stats 
    df_players = df_players.merge(agg_stats, how = "left", on = "PLAYER_ID") 

    # subset to just the columns we want 
    df_players = df_players[[
        "PLAYER_ID", "FULL_NAME", "TEAM_ID", "PTS_MEAN", "PTS_SD", 
        "ROSTER_ID", "IS_STARTER", "SEARCH_RANK"
    ]] 

    # make sure that player_id is an integer 
    df_players["PLAYER_ID"] = df_players["PLAYER_ID"].astype(int) 


    ##### player schedules  ##### 

    # read in the games data 
    df_games = read_s3_csv("all_games", aws_client, bucket_name) 

    # make sure that the game date is formatted correctly 
    df_games["GAME_DATE"] = pd.to_datetime(df_games["GAME_DATE"]) 

    # concatenate the home and away teams 
    df_pgames = pd.concat([
        (
            df_games[["HOME_ID", "GAME_ID", "WEEK_NUMBER", "GAME_DATE"]]
            .rename(columns = {"HOME_ID": "TEAM_ID"})
        ), 
        (
            df_games[["GUEST_ID", "GAME_ID", "WEEK_NUMBER", "GAME_DATE"]]
            .rename(columns = {"GUEST_ID": "TEAM_ID"}) 
        ) 
    ]) 

    # join in a few player attributes 
    df_pgames = df_pgames.merge(
        df_players[["TEAM_ID", "PLAYER_ID", "FULL_NAME"]], 
        on = "TEAM_ID", 
        how = "inner"
    ) 


    ##### locked points  ##### 

    # read in the locked points data 
    df_locks = read_s3_csv("locked_points", aws_client, bucket_name) 

    # filter to just the current week 
    df_locks = df_locks.loc[df_locks["WEEK_NUMBER"] == week_number] 

    # filter to the players that haven't been locked yet 
    df_open = df_locks.loc[df_locks["LOCKED_POINTS"].isna()]

    # filter to the players that have already been locked 
    df_locks = df_locks.loc[~df_locks["LOCKED_POINTS"].isna()] 

    # print("df_locks")
    # print(df_locks) 
    # print("df_open") 
    # print(df_open)


    ##### Matchup Projections  ##### 

    # filter to the players that we need to simulate 
    sim_players = df_players.merge(
        df_open[["PLAYER_ID", "TEAM"]], 
        on = "PLAYER_ID", 
        how = "inner"
    ) 

    # filter to the games left this week 
    current_games = df_pgames.loc[
        (df_pgames["WEEK_NUMBER"] == week_number) & 
        (df_pgames["GAME_DATE"] > pd.Timestamp.now())
    ] 

    # aggregate by player 
    current_games = current_games.groupby("PLAYER_ID").agg(
        GAMES = ("GAME_ID", "count")
    ) 

    # join in the number of games left 
    sim_players = sim_players.merge(current_games, on = "PLAYER_ID", how = "left") 

    # simulate the locked points 
    df_sims = simulate_locked_points(sim_players) 

    # aggregate the simulation reps by team 
    df_reps = df_sims.groupby(["TEAM", "REP_NUM"]).agg( 
        SIM_TOTAL = ("SIM_POINTS", "sum")
    ).reset_index() 

    # summarize the totals that are already locked in 
    locked_totals = df_locks.groupby("TEAM").agg(
        LOCKED_POINTS = ("LOCKED_POINTS", "sum")
    ) 

    # join in the locked points and add to the totals 
    df_reps = df_reps.merge(locked_totals, on = "TEAM", how = "left") 
    df_reps["LOCKED_POINTS"] = df_reps["LOCKED_POINTS"].fillna(0) 
    df_reps["TOTAL_POINTS"] = df_reps["SIM_TOTAL"] + df_reps["LOCKED_POINTS"] 

    # calculate the overall totals 
    df_totals = df_reps.groupby("TEAM").agg(
        LOCKED_POINTS = ("LOCKED_POINTS", "mean"), 
        PCT05 = ("TOTAL_POINTS", lambda x: np.percentile(x, 5)), 
        AVG = ("TOTAL_POINTS", "mean"),
        PCT95 = ("TOTAL_POINTS", lambda x: np.percentile(x, 95))
    ).sort_values("TEAM").reset_index() 

    # calculate the players left to lock 
    dfr = df_open.groupby("TEAM").agg(
        PLAYERS_LEFT = ("PLAYER_ID", "count")
    ).reset_index() 

    # join in the players left to lock 
    df_totals = df_totals.merge(dfr, on = "TEAM", how = "left") 
    df_totals["PLAYERS_LEFT"] = df_totals["PLAYERS_LEFT"].fillna(0) 

    # add the overall totals to the email body 
    email_body += f'''
####################################################################
Matchup Projections: ''' 
    for i, row in df_totals.iterrows():
        email_body += f'''

{row['TEAM']}: {row['AVG']:.1f} points ({row['PCT05']:.1f} - {row['PCT95']:.1f}) 
 - {row['LOCKED_POINTS']:.1f} locked points with {row['PLAYERS_LEFT']:.0f} players left ''' 

    # get the rep totals for my team 
    dfr1 = (
        df_reps.loc[df_reps["TEAM"] == "My Team"] 
        [["REP_NUM", "TOTAL_POINTS"]]
        .rename(columns = {"TOTAL_POINTS": "MY_TEAM"}) 
    ) 

    # get the rep totals for the opponent 
    dfr2 = (
        df_reps.loc[df_reps["TEAM"] == "Opponent"] 
        [["REP_NUM", "TOTAL_POINTS"]] 
        .rename(columns = {"TOTAL_POINTS": "OPPONENT"}) 
    ) 

    # join the two together and calcuate the result 
    df_match = dfr1.merge(dfr2, on = "REP_NUM", how = "inner") 
    df_match["RESULT"] = np.where(df_match["MY_TEAM"] > df_match["OPPONENT"], 1, 0) 

    # calculate the win probability and add to the body 
    win_prob = df_match["RESULT"].mean() 
    email_body += f'''

My Est. Win Probability: {win_prob:.1%} '''  


    ##### Flag Potential Locks  ##### 

    # filter to just the players that we can potentially lock-in 
    df_options = df_open.loc[df_open["TEAM"] == "My Team"][["PLAYER_NAME", "PLAYER_ID"]] 

    # get the games that haven't been played yet  
    dfg2 = (
        df_pgames.loc[
            (df_pgames["PLAYER_ID"].isin(df_options["PLAYER_ID"])) & 
            (df_pgames["WEEK_NUMBER"] == week_number) & 
            (df_pgames["GAME_DATE"] > pd.Timestamp.now()) 
        ][["PLAYER_ID", "GAME_ID"]] 
        .groupby("PLAYER_ID").agg(
            GAMES = ("GAME_ID", "count") 
        ).reset_index() 
    ) 

    # get the most recent fantasy points 
    dfs2 = df_stats.loc[
        (df_stats["PLAYER_ID"].isin(df_options["PLAYER_ID"])) & 
        (df_stats["WEEK_NUMBER"] == week_number)
    ][["PLAYER_ID", "GAME_ID", "FANTASY_POINTS"]] 
    dfs2["RECENT_RANK"] = dfs2.groupby("PLAYER_ID")["FANTASY_POINTS"].rank(ascending = False) 
    dfs2 = dfs2.loc[dfs2["RECENT_RANK"] == 1] 
    dfs2 = dfs2[["PLAYER_ID", "FANTASY_POINTS"]]  

    # join everything together 
    df_options = (
        df_options.merge(dfg2, on = "PLAYER_ID", how = "left") 
        .merge(dfs2, on = "PLAYER_ID", how = "left") 
        .merge(df_players[["PLAYER_ID", "PTS_MEAN", "PTS_SD"]], on = "PLAYER_ID", how = "left") 
    ) 

    # loop through each player and calculate the probability of a better score 
    for i, row in df_options.iterrows(): 
        df_options.loc[i, "PROB_BETTER"] = calc_prob_better(
            current = row["FANTASY_POINTS"], 
            games_left = row["GAMES"], 
            mean = row["PTS_MEAN"], 
            sd = row["PTS_SD"]
        ) 

    # sort by the lock probability 
    df_options = df_options.sort_values("PROB_BETTER").reset_index(drop = True) 

    # flag whether or not we should lock 
    df_options["LOCK"] = np.where(df_options["PROB_BETTER"] < 0.5, "LOCK", "WAIT") 

    # add the lock recommendations to the email body 
    email_body += f'''


####################################################################
Lock-in Recommendations: ''' 
    for i, row in df_options.iterrows():
        email_body += f'''

{row['LOCK']} - {row['PLAYER_NAME']} - {row['FANTASY_POINTS']:.1f} points 
 - {row['PROB_BETTER']:.1%} chance of scoring better ({row['GAMES']:.0f} games with avg {row['PTS_MEAN']:.1f} and sd {row['PTS_SD']:.1f}) ''' 


    ##### potential adds ##### 

    # filter to my players 
    df_mine = df_players.loc[df_players["ROSTER_ID"] == my_roster] 
    df_mine["ROSTER"] = "My Team" 

    # filter to the top 25 free agents 
    df_free = (
        df_players.loc[df_players["ROSTER_ID"].isna()] 
        .sort_values("SEARCH_RANK").head(25) 
    ) 
    df_free["ROSTER"] = "Free Agent" 

    # get the number of games next week 
    dfg2 = (
        df_pgames.loc[df_pgames["WEEK_NUMBER"] == week_number + 1] 
        .groupby("PLAYER_ID").agg(
            GAMES = ("GAME_ID", "count") 
        ).reset_index() 
    ) 

    # put everything together 
    df_compare = (
        pd.concat([df_mine, df_free]) 
        .merge(dfg2, on = "PLAYER_ID", how = "left") 
        [["PLAYER_ID", "FULL_NAME", "PTS_MEAN", "PTS_SD", "GAMES", "ROSTER"]]
    ) 

    # calculate the adjusted projections 
    df_compare = compute_adjusted_projections(df_compare) 

    # sort by the adjusted projections 
    df_compare = df_compare.sort_values("SIM_POINTS", ascending = False).reset_index(drop = True) 

    # filter to the players we should consider 
    max_rank = df_compare.loc[df_compare["ROSTER"] == "My Team"].index.max() 
    df_compare["CONSIDER"] = df_compare.index <= max_rank 
    df_consider = df_compare.loc[df_compare["CONSIDER"]].drop(columns = "CONSIDER") 

    # add the overall totals to the email body 
    email_body += f'''


####################################################################
Potential Adds: ''' 
    for i, row in df_consider.iterrows():
        front_spacing = "    " if row["ROSTER"] == "My Team" else "=== "
        email_body += f'''

{front_spacing}{row['ROSTER']} - {row['FULL_NAME']} 
     - avg {row['PTS_MEAN']:.1f}, sd {row['PTS_SD']:.1f}, {row['GAMES']:.0f} games ''' 


    # print(email_body) 

    # send the email 
    send_emails(user_name, to_email, email_body, secrets_client) 
    