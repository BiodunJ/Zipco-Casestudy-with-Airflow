import pandas as pd

#Data extraction from Azure Blob Storage
def run_extraction():
    try:
        data = pd.read_csv(r'C:\Users\USER\Desktop\Zipco Casestudy with Airflow\zipco_transaction.csv')
        print("Data extracted successfully")
    except Exception as e:
        print(f"Error extracting data: {e}")