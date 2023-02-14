from azure.storage.blob import BlobServiceClient
import os

account_name = os.getenv('STORAGE_ACCOUNT_NAME', "")
account_key = os.getenv('STORAGE_ACCOUNT_KEY', "")
connection_string = os.getenv('CONNECTION_STRING', "")
container_name = "input"

def uploadToBlobStorage(file_path,file_name):
   blob_service_client = BlobServiceClient.from_connection_string(connection_string)
   blob_client = blob_service_client.get_blob_client(container=container_name, blob=file_name)
   with open(file_path,"rb") as data:
      blob_client.upload_blob(data)
      print(f"Uploaded {file_name}.")

import os
dir = "/Users/nealfokam/Documents/M2/Datalake/TP1/stocks_data"
for root, dirs, files in os.walk(dir):
    for f in files:
        uploadToBlobStorage(os.path.join(root, f),f)

