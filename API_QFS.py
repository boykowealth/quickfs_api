import pandas as pd
import requests
from typing import Dict
import logging

class qfs_q:
    def __init__(self, api_key, timeout: int = 600):
        self.api_key = api_key
        self.timeout = timeout
        self.HOST = 'https://public-api.quickfs.net/v1'
        self.resp = None
        
        self.headers = {
            'X-QFS-API-Key': api_key
        }
        
        self.endpoint_pivot = None
        self.QUICKFS_KEYS = ['data']
        self.response_key = None
        self.query_params_names = ["period"]
        self.name_error = False
        self.request_body = None
        self.session = requests.Session()  # Use a session for connection pooling
        
    def __handle_response(self, query_params: Dict[str, str] = {}):
        try:
            if self.request_body is None:
                self.resp = self.session.get(self.endpoint_pivot, params=query_params, headers=self.headers, timeout=self.timeout)
            else:
                self.resp = self.session.post(self.endpoint_pivot, json=self.request_body, headers=self.headers, timeout=self.timeout)
                self.request_body = None  # Reset the request_body after the POST request

            print(" --- Status:", self.resp.status_code, end="\r")
            self.__response_key_finder(self.resp)
            
            if self.resp.status_code == 200:
                # Retrieve only "financials" from JSON data
                json_data = self.resp.json().get(self.response_key, {})
                quarterly_data = json_data.get("financials", {}).get("quarterly", {})
                
                # Convert quarterly data to DataFrame
                if isinstance(quarterly_data, dict):
                    df = pd.json_normalize(quarterly_data)  # Flatten to DataFrame
                else:
                    df = pd.DataFrame(quarterly_data)
                
                # Convert column names to uppercase
                df.columns = [col.upper() for col in df.columns]
                
                # Return the DataFrame
                return df
            else:
                self.resp.raise_for_status()  # Raise an error for bad status codes

        finally:
            if self.resp:
                self.resp.close()  # Close response object to free resources

    def __endpoint_builder(self, endpoint: str, symbol: str = "", all_data: bool = False):
        # Check if `all_data` endpoint is requested
        if all_data:
            self.endpoint_pivot = f"{self.HOST}/data/all-data/{symbol.upper()}"
        else:
            self.endpoint_pivot = f"{self.HOST}{endpoint}/{symbol.upper()}"
    
    def __param_checker(self, items_):
        # Check if query parameters are valid
        for key, value in items_:
            if key not in self.query_params_names:
                logging.error(f"The parameter {key} is not valid.")
                self.name_error = True

    def __response_key_finder(self, response):
        try:
            for key in response.json().keys():
                if key in self.QUICKFS_KEYS:
                    self.response_key = key
                    return
            logging.error("None of the expected keys found in the response.")
            self.response_key = None
        except Exception as e:
            logging.error(f"Error parsing response JSON: {e}")
            self.response_key = None

    def data(self, symbol: str, metric: str = None, all_data: bool = False, **query_params):
        """
        Fetch data from the API and return only financial data as a pandas DataFrame.
        
        Args:
            symbol (str): Ticker symbol.
            metric (str): Metric for specific data pull.
            all_data (bool): If True, fetches 'all-data' endpoint.
            query_params: Additional parameters like period.
        """
        
        # Set the endpoint to all-data or specific metric
        if all_data:
            self.__endpoint_builder(f"/data", symbol=symbol, all_data=True)
        else:
            self.__endpoint_builder(f"/data/{symbol.upper()}/{metric.lower()}")
        
        # Check for valid parameters
        self.__param_checker(items_=query_params.items())
        
        if self.name_error:
            self.name_error = False
            return
        
        # Make the request and return only financial data as DataFrame
        df = self.__handle_response(query_params)
        
        # Return an empty DataFrame if no data was found
        if df.empty:
            return pd.DataFrame()
        
        # Explode the DataFrame so each element in the lists becomes its own row
        df_exploded = df.apply(lambda x: x.explode() if x.dtype == 'object' else x)
        df_exploded = df_exploded.reset_index(drop=True)  # Reset index for clean output

        return df_exploded

class qfs_y:
    def __init__(self, api_key, timeout: int = 600):
        self.api_key = api_key
        self.timeout = timeout
        self.HOST = 'https://public-api.quickfs.net/v1'
        self.resp = None
        
        self.headers = {
            'X-QFS-API-Key': api_key
        }
        
        self.endpoint_pivot = None
        self.QUICKFS_KEYS = ['data']
        self.response_key = None
        self.query_params_names = ["period"]
        self.name_error = False
        self.request_body = None
        self.session = requests.Session()  # Use a session for connection pooling
        
    def __handle_response(self, query_params: Dict[str, str] = {}):
        try:
            if self.request_body is None:
                self.resp = self.session.get(self.endpoint_pivot, params=query_params, headers=self.headers, timeout=self.timeout)
            else:
                self.resp = self.session.post(self.endpoint_pivot, json=self.request_body, headers=self.headers, timeout=self.timeout)
                self.request_body = None  # Reset the request_body after the POST request

            print(" --- Status:", self.resp.status_code, end="\r")
            self.__response_key_finder(self.resp)
            
            if self.resp.status_code == 200:
                # Retrieve only "financials" from JSON data
                json_data = self.resp.json().get(self.response_key, {})
                quarterly_data = json_data.get("financials", {}).get("annual", {})
                
                # Convert quarterly data to DataFrame
                if isinstance(quarterly_data, dict):
                    df = pd.json_normalize(quarterly_data)  # Flatten to DataFrame
                else:
                    df = pd.DataFrame(quarterly_data)
                
                # Convert column names to uppercase
                df.columns = [col.upper() for col in df.columns]
                
                # Return the DataFrame
                return df
            else:
                self.resp.raise_for_status()  # Raise an error for bad status codes

        finally:
            if self.resp:
                self.resp.close()  # Close response object to free resources

    def __endpoint_builder(self, endpoint: str, symbol: str = "", all_data: bool = False):
        # Check if `all_data` endpoint is requested
        if all_data:
            self.endpoint_pivot = f"{self.HOST}/data/all-data/{symbol.upper()}"
        else:
            self.endpoint_pivot = f"{self.HOST}{endpoint}/{symbol.upper()}"
    
    def __param_checker(self, items_):
        # Check if query parameters are valid
        for key, value in items_:
            if key not in self.query_params_names:
                logging.error(f"The parameter {key} is not valid.")
                self.name_error = True

    def __response_key_finder(self, response):
        try:
            for key in response.json().keys():
                if key in self.QUICKFS_KEYS:
                    self.response_key = key
                    return
            logging.error("None of the expected keys found in the response.")
            self.response_key = None
        except Exception as e:
            logging.error(f"Error parsing response JSON: {e}")
            self.response_key = None

    def data(self, symbol: str, metric: str = None, all_data: bool = False, **query_params):
        """
        Fetch data from the API and return only financial data as a pandas DataFrame.
        
        Args:
            symbol (str): Ticker symbol.
            metric (str): Metric for specific data pull.
            all_data (bool): If True, fetches 'all-data' endpoint.
            query_params: Additional parameters like period.
        """
        
        # Set the endpoint to all-data or specific metric
        if all_data:
            self.__endpoint_builder(f"/data", symbol=symbol, all_data=True)
        else:
            self.__endpoint_builder(f"/data/{symbol.upper()}/{metric.lower()}")
        
        # Check for valid parameters
        self.__param_checker(items_=query_params.items())
        
        if self.name_error:
            self.name_error = False
            return
        
        # Make the request and return only financial data as DataFrame
        df = self.__handle_response(query_params)
        
        # Return an empty DataFrame if no data was found
        if df.empty:
            return pd.DataFrame()
        
        # Explode the DataFrame so each element in the lists becomes its own row
        df_exploded = df.apply(lambda x: x.explode() if x.dtype == 'object' else x)
        df_exploded = df_exploded.reset_index(drop=True)  # Reset index for clean output

        return df_exploded



