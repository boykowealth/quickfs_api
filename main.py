import pandas as pd
import numpy as np
from API_QFS import qfs_q as QFS_API, qfs_y
import datetime as dt

global api
api = 'a83213f6d2d4467d012e10646b1f73cf47324116'

class FS:

    @staticmethod
    def quarterly(ticker):
        qfs_instance = QFS_API(api_key=api)
        qfs_data = qfs_instance.data(symbol=ticker, all_data=True)
        return qfs_data
    
    @staticmethod
    def annual(ticker):
        qfs_y_instance = qfs_y(api_key=api)
        qfs_y_data = qfs_y_instance.data(symbol=ticker, all_data=True)
        return qfs_y_data
    
    @staticmethod
    def company_list():
        comp_list = pd.read_feather(r"C:\Users\bboyk\OneDrive\BOYKO TERMINAL\PROGRAMS\Data\Data Hub\Equity\Equity_Firm\Firms\Firm_List.feather")
        return comp_list

    @staticmethod
    def downcast_df(df):
        # Downcast integers and floats to minimize memory usage
        for col in df.select_dtypes(include=['int', 'float']).columns:
            df[col] = pd.to_numeric(df[col], downcast='integer' if df[col].dtype == 'int' else 'float')
        return df
    
    @staticmethod
    def db_quarterly():
        data = FS.company_list()
        # Initialize an empty list to store results
        results = []

        # Loop through each row in the DataFrame
        for index, row in data.iterrows():
            ticker = row['QFS_ID']  # Adjust the column name if necessary
            print(f"Fetching quarterly data for ticker: {ticker}", end='/r')  # Print the current ticker
            quarterly_data = FS.quarterly(ticker)
            results.append(quarterly_data)
        
        # Concatenate the results into a single DataFrame
        combined_results = pd.concat(results, ignore_index=True)
        # Downcast before saving
        combined_results = FS.downcast_df(combined_results)
        return combined_results
    
    @staticmethod
    def db_annual():
        data = FS.company_list()
        # Initialize an empty list to store results
        results = []

        # Loop through each row in the DataFrame
        for index, row in data.iterrows():
            ticker = row['QFS_ID']  # Adjust the column name if necessary
            print(f"Fetching quarterly data for ticker: {ticker}", end='/r')  # Print the current ticker
            annual_data = FS.annual(ticker)
            results.append(annual_data)
        
        # Concatenate the results into a single DataFrame
        combined_results = pd.concat(results, ignore_index=True)
        # Downcast before saving
        combined_results = FS.downcast_df(combined_results)
        return combined_results
    
    @staticmethod
    def main():
        timer = dt.time()
        print(timer)
        
        # Process and save quarterly results as Parquet
        quarterly_results = FS.db_quarterly()
        quarterly_results.to_parquet(r"C:\Users\bboyk\OneDrive\BOYKO TERMINAL\PROGRAMS\Data\Data Hub\Equity\Equity_Firm\Financials\Quarterly\ALL_DATA.parquet")
        print(quarterly_results)
        print("Quarterly Complete In:", (dt.time() - timer) / 3600, "Hours")
        
        # Reset the timer
        timer = dt.time()
        
        # Process and save annual results as Parquet
        annual_results = FS.db_annual()
        annual_results.to_parquet(r"C:\Users\bboyk\OneDrive\BOYKO TERMINAL\PROGRAMS\Data\Data Hub\Equity\Equity_Firm\Financials\Yearly\ALL_DATA.parquet")
        print(annual_results)
        print("Annual Complete In:", (dt.time() - timer) / 3600, "Hours")

FS.main()
