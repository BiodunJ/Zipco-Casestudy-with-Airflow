# importing necessary libraries
import pandas as pd
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
import os
import datetime

#Data Loading
def run_loading():
# Load the cleaned data from the CSV file
    data = pd.read_csv('cleaned_data.csv')
    products = pd.read_csv('products.csv')
    customers = pd.read_csv('customers.csv')
    staff = pd.read_csv('staff.csv')
    transactions = pd.read_csv('transactions.csv')

    print("Data loaded successfully")

# Load environment variables from .env file
    load_dotenv()

    connect_str = os.getenv("AZURE_CONNECTION_STRING_VALUE")
    container_name = os.getenv("CONTAINER_NAME")

    #create a BlobServiceClient objcet
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
# Create a container
    container_client = blob_service_client.get_container_client(container_name)

# Upload the CSV files to the Azure Blob Storage container
    files = [
        (data, 'rawdata/cleaned_zipco_transaction_data.csv'),
        (products, 'cleaneddata/products.csv'),
        (customers, 'cleaneddata/customers.csv'),
        (staff, 'cleaneddata/staff.csv'),
        (transactions, 'cleaneddata/transactions.csv')
    ]

    for file, blob_name in files:
        blob_client = container_client.get_blob_client(blob_name)
        output = file.to_csv(index=False)  # Save to a temporary CSV file
        blob_client.upload_blob(output, overwrite=True)
        print(f"Uploaded {blob_name} loaded into Azure Blob Storage")