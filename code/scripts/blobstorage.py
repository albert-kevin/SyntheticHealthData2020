# pip install azure-storage-blob pandas pyarrow fastparquet
# old version v2.1  : https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python-legacy
# new version v12.x : https://docs.microsoft.com/en-us/azure/storage/blobs/storage-quickstart-blobs-python 
# https://azuresdkdocs.blob.core.windows.net/$web/python/azure-storage-blob/12.5.0/index.html

# load latest module
from azure.storage.blob import ContainerClient # pip install azure-storage-blob
import pandas as pd # pip install pandas pyarrow fastparquet
from io import BytesIO

# variables
adlsGen2AccountName = "datalake27112020"
adlsGen2Key = "WJ4lTl5w9ze3hberxHsnNoWON5DTZJyajggoLY3j7WgsDDFm5w/NPuDAfO4Po/bNellztxilXm2Gpo9GzEzxdA=="
adlsGen2ContainerName = "datalake"
adlsGen2AccountURL = "https://"+adlsGen2AccountName+".blob.core.windows.net/"

# client registration
container_client = ContainerClient(adlsGen2AccountURL, adlsGen2ContainerName, adlsGen2Key)

# read blob file to DataFrame (*.parquet)
def read_blob(destination_path):
    blob = container_client.download_blob(destination_path)
    data = BytesIO(blob.content_as_bytes())
    return pd.read_parquet(data)

# write DataFrame to blob file (*.parquet)
def write_blob(destination_path, df):
    buffer = BytesIO()
    df.to_parquet(buffer)
    container_client.upload_blob(destination_path, buffer.getvalue(), overwrite=True)

# copy blob file to local disk
def copy_blob(source_path, destination_path):
    with open(destination_path, "wb") as download_file:
        download_file.write(container_client.download_blob(source_path).readall())

# # test
# df = pd.read_csv("../../data/platinum/diabetes.csv") # read sample data from local disk
# print(df.head(2))
# # 1. write sample dataframe to blobstorage file and new folder (*.parquet)
# write_blob("platinum/sample.parquet", df)
# # 2. read blob file to DataFrame (*.parquet) 
# print(read_blob("platinum/sample.parquet").head(2))
# # 3. copy blob file to local disk (*.parquet)
# copy_blob("platinum/sample.parquet", "downloaded.parquet")
# print(pd.read_parquet("downloaded.parquet").head(2))