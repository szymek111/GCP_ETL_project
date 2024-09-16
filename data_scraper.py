import yfinance as yf
import csv
from google.cloud import storage
from bs4 import BeautifulSoup
import requests, os
import pandas as pd


class DataScraper:
    def __init__(self, ticker):
        self.ticker = ticker
        self.df = None
        self.engine = None
        self.info = None
        self.stock = None

    def choose_stock(self):
        self.stock = yf.Ticker(self.ticker)
        self.get_stock_info()
        return self.stock
    
    def scrap_df(self, start_date=None, end_date=None):
        self.df = self.stock.history(start=start_date, end=end_date)
        self.df.reset_index(inplace=True)
        print(f"Dataframe for -{self.ticker}- created for:\nStart date: {start_date}\nEnd date: {end_date}")
        return self.df
    
    def get_stock_info(self):
        self.info = self.stock.info
        return self.info

    def set_engine(self, engine):
        self.engine = engine
        return self.engine

    def add_df(self):
        self.df.to_sql(name=self.ticker, con=self.engine, if_exists='replace', index=False)
        return print(f"'{self.info['shortName']}' added to database\n")
    
class GCPUploader:
    def __init__(self, bucket_name, source_file_name, destination_blob_name):
        self.bucket_name = bucket_name
        self.source_file_name = source_file_name
        self.destination_blob_name = destination_blob_name
        
        # Upload the CSV file to a GCS bucket
    def upload_to_gcs(self):
        storage_client = storage.Client()
        bucket = storage_client.bucket(self.bucket_name)
        blob = bucket.blob(self.destination_blob_name)

        blob.upload_from_filename(self.source_file_name)

        print(f'File {self.source_file_name} uploaded to {self.destination_blob_name} in {self.bucket_name}.')


    
if __name__ == '__main__':

    with open('wig20_components.csv', newline='\n') as csv_file:
        ticker_list = csv.reader(csv_file)
        for ticker in ticker_list:
            DS_obj = DataScraper(ticker[0])
            DS_obj.choose_stock()
            DS_obj.scrap_df()
            DS_obj.df.to_csv(f'WIG20_bucket/{ticker[0]}.csv')
            print(f'{DS_obj.info['exchange']}, {DS_obj.info['longName']}')

            GCPU_obj = GCPUploader('bucket-etl-data', f'WIG20_bucket/{ticker[0]}.csv', f'{ticker[0]}.csv')
            GCPU_obj.upload_to_gcs()