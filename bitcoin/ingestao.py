#%%
from abc import ABC, abstractmethod
import requests
import logging
import datetime
import json
from typing import List
import os
from schedule import repeat, every, run_pending
import time

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

# %%
class MercadoBitcoinApi(ABC):

    def __init__(self, coin: str) -> None:
        self.coin = coin
        self.base_endpoint = "https://www.mercadobitcoin.net/api"
    
    @abstractmethod
    def _get_endpoint(self,**kwargs) -> str:
        pass
    
    def get_data(self,**kwargs) -> dict:
        endpoint = self._get_endpoint(**kwargs)
        logger.info(f"Getting data from endpoint: {endpoint}")
        response = requests.get(endpoint)
        response.raise_for_status()
        return response.json()
    
class DaySummaryApi(MercadoBitcoinApi):
    type= "day-summary"
    
    def _get_endpoint(self, date:datetime.date) -> str:
        return f"{self.base_endpoint}/{self.coin}/{self.type}/{date.year}/{date.month}/{date.day}"
    
class TradesApi(MercadoBitcoinApi):
    type = "trades"
    
    def _get_unix_epoch(self,date: datetime.datetime) -> int:
        return int(date.timestamp())
    
    def _get_endpoint(self, date_from: datetime.datetime = None, date_to: datetime.datetime = None) -> str:
        if date_from and not date_to:
            unix_date_from = self._get_unix_epoch(date_from)
            endpoint = f'{self.base_endpoint}/{self.coin}/{self.type}/{unix_date_from}'
        elif date_from and date_to:
            unix_date_from = self._get_unix_epoch(date_from)
            unix_date_to = self._get_unix_epoch(date_to)
            endpoint = f'{self.base_endpoint}/{self.coin}/{self.type}/{unix_date_from}/{unix_date_to}'
        else:
            endpoint = f'{self.base_endpoint}/{self.coin}/{self.type}'
        
        return endpoint
        

class DataTypeNotSupportedForIngestionException(Exception):
    def __init__(self,data):
        self.data = data
        self.message = f"Data type {type(data)} is not supported for ingestion"
        super().__init__(self.message)

class DataWriter:
    
    def __init__(self, coin: str, api : str) -> None:
        self.api = api
        self.coin = coin
        self.filename = f"{self.api}/{self.coin}/{datetime.datetime.now()}.json"     
    
    def _write_row(self,row : str) -> None:
        os.makedirs(os.path.dirname(self.filename),exist_ok=True)
        with open(self.filename, "a") as f:
            f.write(row)
    
    def write(self, data: [list, dict]):
        if isinstance(data,dict):
            self._write_row(json.dumps(data) + "\n")
        elif isinstance(data,list):
            for element in data:
                self.write(element)  
        else:
            raise DataTypeNotSupportedForIngestionException(data)


class DataIngestor(ABC):
    
    def __init__(self,writer: DataWriter, coins: List[str], default_start_date: datetime.date) -> None:
        self.default_start_date = default_start_date
        self.coins = coins
        self.writer = writer
        self._checkpoint = None
    
    @property   
    def _checkpoints_filename(self) -> str:
        return f"{self.__class__.__name__}.checkpoint"
    
    def _write_checkpoint(self):
        with open(self._checkpoints_filename,'w') as f:
            f.write(f"{self._checkpoint}")
            
    def _load_checkpoint(self) -> datetime:
        try:
            with open(self._checkpoints_filename,'r') as f:
                return datetime.datetime.strptime(f.read(),"%Y-%m-%d").date()
        except FileNotFoundError:
            return None
        
    def _get_checkpoint(self):
        if not self._checkpoint:
            return self.default_start_date
        else:
            return self._checkpoint
        
    def _update_checkpoint(self,value):
        self._checkpoint = value
        self._write_checkpoint()
        
    @abstractmethod
    def ingest(self) -> None:
        pass
  
class DaySummaryIngestor(DataIngestor):
    def ingest(self) -> None:
        date = self._get_checkpoint()
        if date < datetime.date.today():
            for coin in self.coins:
                api = DaySummaryApi(coin=coin)
                data = api.get_data(date=date)
                self.writer(coin=coin,api=api.type).write(data)
            self._update_checkpoint(date + datetime.timedelta(days=1))

#%%
ingestor = DaySummaryIngestor(writer=DataWriter,coins=["BTC","ETH","LTC"],default_start_date=datetime.date(2021,6,1))

# %%
@repeat(every(1).seconds)
def job():
    ingestor.ingest()
    
while True:
    run_pending()
    time.sleep(0.5)
    
# %%
