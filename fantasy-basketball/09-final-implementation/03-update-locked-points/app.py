
import boto3 
import json 
import os 

import string 
import openpyxl 
import requests 

from io import BytesIO 
import pandas as pd 
import numpy as np 
from azure.identity import ClientSecretCredential 



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


def download_excel_file(file_url, file_name, headers):

    # make the API request 
    response = requests.get(file_url, headers = headers)

    # check if the request was successful 
    if response.status_code == 200:

        # if successful, load the workbook with openpyxl 
        file = BytesIO(response.content) 
        wb = openpyxl.load_workbook(file) 

        # update the user 
        print("File downloaded successfully.") 

        return wb 
    
    # if it wasn't, prompt the user and showcase the error message 
    else:
        print(f"Failed to download file. Status code: {response.status_code}") 
        return None 


def upload_excel_file(file_url, wb, headers):

    # Save the updated workbook to a BytesIO object
    output = BytesIO()
    wb.save(output)
    output.seek(0) 

    # uploaded the file to SharePoint 
    response = requests.put(file_url, headers = headers, data = output)

    # check if the upload was successful and prompt the user accordingly 
    if response.status_code == 200:
        print("File uploaded successfully.")
    else:
        print(f"Failed to upload file. Status code: {response.status_code}")


# function to retrieve the player id based on the full name 
def get_player_id(sheet, name_cell, df_players):

    # get the player name 
    player_name = sheet[name_cell].value 

    # filter to the current player by name 
    current_player = df_players.loc[df_players["FULL_NAME"] == player_name.upper()] 

    # get the player id if the player could be found 
    if len(current_player.index) > 0:
        player_id = int(current_player["PLAYER_ID"].values[0]) 
    
    # otherwise, return None with an error message 
    else:
        print(f"Player {player_name} not found!") 
        sheet[name_cell].value = f"ERROR: Player '{player_name}' Not Found!" 
        player_id = None 
    
    return player_name, player_id 


# function to update the stats for a given player 
def update_player_stats(sheet, player_id, week_number, base_row, cols, df_pgames):

    # filter the stats data for this player/week 
    current_stats = df_pgames.loc[
        (df_pgames["PLAYER_ID"] == player_id) &
        (df_pgames["WEEK_NUMBER"] == week_number) 
    ] 

    # add the day number column 
    current_stats["DAY_NUM"] = current_stats["GAME_DATE"].dt.dayofweek 

    # loop through each day of the week 
    for i in range(7): 
        day_stats = current_stats.loc[current_stats["DAY_NUM"] == i] 

        # date and time cells 
        date_cell = f"{cols[i + 1]}{base_row - 2}" 
        time_cell = f"{cols[i + 1]}{base_row - 1}" 
        stat_cell = f"{cols[i + 1]}{base_row}" 

        # fill in the info if there was a game this day 
        if len(day_stats.index) > 0:

            # get the date/stats 
            game_date = pd.to_datetime(day_stats["GAME_DATE"].values[0]) 
            game_stat = day_stats["FANTASY_POINTS"].values[0]  

            # fill in blanks for null stats 
            if np.isnan(game_stat):
                game_stat = "" 

            # fill in the info 
            sheet[date_cell].value = game_date.strftime("%a %m/%d") 
            sheet[time_cell].value = game_date.strftime("%I:%M %p") 
            sheet[stat_cell].value = game_stat 
        
        # otherwise, fill in blanks 
        else:
            sheet[date_cell].value = "" 
            sheet[time_cell].value = "" 
            sheet[stat_cell].value = "" 


# funtion to get the locked points based on the spreadsheet 
def get_locked_points(sheet, base_row, cols):

    # placeholder for the locked value 
    locked_points = np.nan  

    # loop through each day of the week and check for locked games 
    for i, c in enumerate(cols[2:]): 
        lock_val = sheet[f"{c}{base_row+1}"].value 
        if (lock_val is not None) and (lock_val != ""): 
            try:
                locked_points = float(sheet[f"{c}{base_row}"].value)  
                break 
            except: 
                pass  
    
    return locked_points 


# handler function 
def lambda_handler(event, context):
    
    # turn off the pd chained assignment warning 
    pd.options.mode.chained_assignment = None 

    # AWS file locations 
    bucket_name = "fantasy-basketball-data" 

    # create the clients that we need  
    aws_client = boto3.client('s3') 
    secrets_client = boto3.client('secretsmanager') 

    # get the Service Principal credentials  
    secret_client = secrets_client.get_secret_value(SecretId = "azureSPCredentials") 
    secrets = json.loads(secret_client["SecretString"]) 
    client_id = secrets["CLIENT_ID"] 
    client_secret = secrets["CLIENT_SECRET"] 
    tenant_id = secrets["TENANT_ID"] 

    # file parameters 
    host_name = "dailydataapps" 
    site_name = "FantasyBasketballData" 
    drive_name = "Documents" 
    file_name = "Enter Lock-ins.xlsx" 

    # file paths 
    sharepoint_file = f"Analytics//{file_name}" 

    # base URL for the Microsoft Graph API 
    base_url = "https://graph.microsoft.com"

    # generate an access token 
    scopes = [f"{base_url}/.default"] 
    credentials = ClientSecretCredential(tenant_id, client_id, client_secret) 
    access_token = credentials.get_token(*scopes).token 

    # add the token to a headers dictionary 
    headers = {
        "Authorization": f"Bearer {access_token}" 
    } 

    # Get the SharePoint site ID 
    site_url = f"{base_url}/v1.0/sites/{host_name}.sharepoint.com:/sites/{site_name}"
    site_response = requests.get(site_url, headers = headers)  
    site_id = site_response.json()['id'] 

    # Get the drive ID
    drive_url = f"{base_url}/v1.0/sites/{site_id}/drives"
    drive_response = requests.get(drive_url, headers = headers)
    drive_id = drive_response.json()['value'][0]['id'] 

    # Define the spreadsheet URL
    file_url = f"{base_url}/v1.0/sites/{site_id}/drives/{drive_id}/root:/{sharepoint_file}:/content" 

    # download the Excel file 
    wb = download_excel_file(file_url, file_name, headers) 


    ##### players data #####

    # read in the players data 
    df_players = read_s3_csv("raw_players", aws_client, bucket_name) 

    # put together a full name column 
    df_players["FULL_NAME"] = (df_players["FIRST_NAME"] + " " + df_players["LAST_NAME"]).str.upper() 


    ##### games data ##### 

    # read in the games data 
    df_games = read_s3_csv("all_games", aws_client, bucket_name) 

    # make sure that the game date is formatted correctly 
    df_games["GAME_DATE"] = pd.to_datetime(df_games["GAME_DATE"]) 


    ##### games data ##### 

    # read in the stats data 
    df_stats = read_s3_csv("all_stats", aws_client, bucket_name) 

    # make sure that the game date is formatted correctly 
    df_stats["GAME_DATE"] = pd.to_datetime(df_stats["GAME_DATE"]) 


    ##### player schedules ##### 

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

    # join in the stats if they exist 
    df_pgames = df_pgames.merge(
        df_stats[["GAME_ID", "PLAYER_ID", "FANTASY_POINTS"]], 
        on = ["GAME_ID", "PLAYER_ID"], 
        how = "left" 
    ) 

    
    ##### update the Excel file ###### 

    # columns for our team vs the opponent 
    team_cols = {
        "My Team": string.ascii_uppercase[1:9], 
        "Opponent": string.ascii_uppercase[10:18] 
    } 

    # other constants 
    nplayers = 9  
    row_start = 7 
    rows_between = 5 

    # placeholder for the locked points 
    df = pd.DataFrame() 

    # loop through each sheet 
    for i, sheetname in enumerate(wb.sheetnames):
        if "Week" in sheetname:

            # get the current sheet and week number 
            sheet = wb[sheetname] 
            week_number = int(sheetname.split()[-1]) 

            # loop through the different teams 
            for tname, cols in team_cols.items():

                # loop through each of the players 
                for pnum in range(nplayers):
                    base_row = row_start + (pnum * rows_between)  

                    # get the player name and id 
                    player_name, player_id = get_player_id(
                        sheet = sheet, 
                        name_cell = f"{cols[0]}{base_row}", 
                        df_players = df_players 
                    ) 

                    # make sure that the player was found 
                    if "ERROR" not in player_name:

                        # update the player stats 
                        update_player_stats(sheet, player_id, week_number, base_row, cols, df_pgames) 

                        # get the locked points 
                        locked_points = get_locked_points(sheet, base_row, cols) 

                        # update the locked points dataframe 
                        df = pd.concat([df, pd.DataFrame({
                            "WEEK_NUMBER": [week_number], 
                            "TEAM": tname, 
                            "PLAYER_ID": player_id, 
                            "PLAYER_NAME": player_name, 
                            "LOCKED_POINTS": locked_points 
                        })], ignore_index = True) 

    # Upload the modified Excel file 
    upload_excel_file(file_url, wb, headers) 
    
    # save the locked points to S3 
    csv_string = df.to_csv(index = False) 
    aws_client.put_object(
        Bucket = bucket_name, 
        Key = "locked_points.csv", 
        Body = csv_string
    ) 