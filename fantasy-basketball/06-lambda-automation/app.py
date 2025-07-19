
import json 
import boto3   

import pandas as pd 

def create_dataframe():

    # create the dataframe 
    df = pd.DataFrame({
        "COL1": [1, 2, 3, 4, 5],
        "COL2": [10, 20, 30, 40, 50],
        "COL3": [100, 200, 300, 400, 500] 
    })

    return df 

# handler function 
def lambda_handler(event, context):


    # create the AWS Secret Manager client 
    secrets_client = boto3.client('secretsmanager') 

    # retrieve your secrets 
    secret = secrets_client.get_secret_value(SecretId = "demoSecrets") 
    secret_vals = json.loads(secret["SecretString"]) 
    secret1 = secret_vals["secret1"] 
    secret2 = secret_vals["secret2"] 

    # double check that we got the secrets 
    #  (you normally wouldn't want to print these) 
    print(f"Secret 1: {secret1}") 
    print(f"Secret 2: {secret2}") 


    # create the dataframe 
    df = create_dataframe() 

    # perform some operations on the dataframe 
    df["COL4"] = df["COL1"] + df["COL2"] 
    df["COL5"] = df["COL3"] * 2 
    df["COL6"] = df["COL4"] / df["COL5"] 

    # showcase the dataframe 
    print("DataFrame after operations:") 
    print(df) 


    # create the AWS S3 client 
    aws_client = boto3.client('s3') 

    # upload the dataframe to S3 as a CSV file
    csv_buffer = df.to_csv(index=False) 
    aws_client.put_object(
        Bucket = "demo-write-data",
        Key = "demo_dataframe.csv",
        Body = csv_buffer
    ) 

