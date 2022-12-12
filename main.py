from azure.storage.blob import BlobClient, BlobBlock
import pandas as pd

connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakesprojectaccount;AccountKey=J8SEcu35jwdkdJPv3hg78WBgkoHc1hM0BEzu9vqfoNbEW/2zNqPEtQWM1FJqkkqhhcxHcWLn2AII+AStUhq1EQ==;EndpointSuffix=core.windows.net"

blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="processed", blob_name="stocks.csv")

if blob.exists():
    blob.delete_blob()

df = pd.concat(map(pd.read_csv, ["AMAZON.csv", "APPLE.csv", "FACEBOOK.csv", "GOOGLE.csv", "MICROSOFT.csv", "TESLA.csv", "ZOOM.csv"]), ignore_index=True)

df.to_csv("stocks.csv", index=False)

with open("stocks.csv", "rb") as data:
    blob.upload_blob(data)