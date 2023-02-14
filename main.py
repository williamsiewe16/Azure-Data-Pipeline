from azure.storage.blob import BlobServiceClient, BlobClient, BlobBlock
import pandas as pd

connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakesprojectaccount;AccountKey=J8SEcu35jwdkdJPv3hg78WBgkoHc1hM0BEzu9vqfoNbEW/2zNqPEtQWM1FJqkkqhhcxHcWLn2AII+AStUhq1EQ==;EndpointSuffix=core.windows.net"
container_name = "input"

blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="processed", blob_name="stocks.csv")

def uploadToBlobStorage(file_path,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
   with open(file_path,"rb") as data:
      blob_client.upload_blob(data)
      print(f"Uploaded {file_name}.")

import os
dir = "/stocks_data"
for root, dirs, files in os.walk(dir):
    for f in files:
        uploadToBlobStorage(os.path.join(root, f),f)

if blob.exists():
    blob.delete_blob()

df = pd.concat(map(pd.read_csv, ["AMAZON.csv", "APPLE.csv", "FACEBOOK.csv", "GOOGLE.csv", "MICROSOFT.csv", "TESLA.csv", "ZOOM.csv"]), ignore_index=True)

df.to_csv("stocks.csv", index=False)

with open("stocks.csv", "rb") as data:
    blob.upload_blob(data)