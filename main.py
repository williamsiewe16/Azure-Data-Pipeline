from azure.storage.blob import BlobServiceClient, BlobClient, BlobBlock
import pandas as pd

connection_string = "DefaultEndpointsProtocol=https;AccountName=datalakesprojectaccount;AccountKey=eHUagFYoTJhe8sooRNUaHcSKoKt8YpYXHrrodcBDD7SHwI7ehyBLOZclFznR0JWdsSq/3L5UZJJu+ASt6le6cg==;EndpointSuffix=core.windows.net"

blob = BlobClient.from_connection_string(conn_str=connection_string, container_name="processed", blob_name="stocks.csv")


if blob.exists():
    blob.delete_blob()

df = pd.concat(map(pd.read_csv, ["AMAZON.csv", "APPLE.csv", "FACEBOOK.csv", "GOOGLE.csv", "MICROSOFT.csv", "TESLA.csv", "ZOOM.csv"]), ignore_index=True)

df.to_csv("stocks.csv", index=False)

with open("stocks.csv", "rb") as data:
    blob.upload_blob(data)