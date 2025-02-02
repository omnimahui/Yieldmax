#!/opt/anaconda3/bin/python

import requests
import json
from munch import DefaultMunch
import re
from datetime import datetime, timedelta, date
import numpy as np
#numpy.random._bit_generator = numpy.random.bit_generator
import pandas as pd
import pprint
import time
import scipy.stats
import math
import finnhub
from IPython.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))
from pretty_html_table import build_table
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP
import smtplib
import sys
import os
import base64
from settings import  (SCHWAB_APP_KEY,
                       SCHWAB_APP_SECRET,
                       SCHWAB_REFRESH_TOKEN,
                       FINNHUB_KEY,
                       SMTP_SERVER,
                       SMTP_PORT,
                       SMTP_USER,
                       SMTP_PASS)


UNIFIED_OPTION_PATTERN = r'([a-zA-Z]+)(\d*)_(\d+)([C|P])(\d+\.?\d*)'
YIELDMAX_OPTION_PATTERN =  r'\d*([a-zA-Z]+)\s+(\d+)([C|P])(\d+\.?\d*)\d{2}'

def getERdate2(symbol):
    finnhub_client = finnhub.Client(api_key=FINNHUB_KEY)
    try: 
        er_dates = (finnhub_client.earnings_calendar(_from=date.today() - timedelta(days=1), to=date.today()+timedelta(days=100), 
                                                     symbol=symbol, international=False))
    except:
        er_dates = {}
    if not er_dates or not er_dates['earningsCalendar']:
        return '2099-12-31'
    else:
        return er_dates['earningsCalendar'][-1]['date']


class Option():
    #symbol requires unified format
    def __init__(self, symbol):
        m = re.compile(YIELDMAX_OPTION_PATTERN).search(symbol)
        self.underlying = m.group(1)
        self.exp = datetime.strptime(m.group(2), '%y%m%d')
        self.callput = 'CALL' if m.group(3) == 'C'  else 'PUT'
        self.strike = float(m.group(4)) / 10
        self.price = 0
        self.underlyingPrice = 0
        self.daysToExpiration = 0
        self.intrinsic = 0
        self.extrinsic = 0
        self.itm = 0
        self.actionNeed = 0
        self.daysToER = 0
        self.delta = 0
        self.gamma = 0
        self.theta = 0
        self.vega = 0
        self.openInterest = 0
        self.volatility = 0
        
        
        

class Position():
    def __init__(self, symbol, equity_type, quantity, weight, etf=""):
        self.symbol = symbol
        self.equity_type = equity_type
        self.quantity = quantity
        self.property = None
        self.etf = etf
        self.weight =  weight

class Portfolio():
    def __init__(self):
        self.portf_list = []
        return
    
    def add(self, new_pos: Position):
        for pos in  self.portf_list:
            if pos.symbol == new_pos.symbol and \
               pos.equity_type == new_pos.equity_type:
                pos.quantity += new_pos.quantity
                return 

        self.portf_list.append(new_pos)
        return

    #def get(self, symbol="", equity_type=""):
        
class Exchange():
    def __init__(self):
        self.positions = dict()
        self.pos_list = []
        return
    
    def auth(self):
        return
    
class Schwab(Exchange):
    def __init__(self):
        super().__init__()
        self.access_token = ""
        self.account_number = ""
        self.base_url = "https://api.schwabapi.com"
        self.option_pattern = r'([a-zA-Z]+)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)'
        self.auth()
        
    def auth(self) -> str:
        headers = {
            "Authorization": f'Basic {base64.b64encode(f"{SCHWAB_APP_KEY}:{SCHWAB_APP_SECRET}".encode()).decode()}',
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": SCHWAB_REFRESH_TOKEN,
        }    
        response = requests.post(
            url="https://api.schwabapi.com/v1/oauth/token",
            headers=headers,
            data=payload,
        )
        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            return self.access_token
        else:
            return ""        
        
    def get_account_number_hash_value(self) -> str:
        url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
        response = self.send_request(url)
        self.account_number = response[0].hashValue
        return self.account_number
    
    def parse_positions(self, positions):
        l = []
        for pos in positions:
            if pos.instrument.assetType == 'OPTION':
                try:
                    m = re.compile(self.option_pattern).search(pos.instrument.symbol)
                    underlying = m.group(1)
                    optional_digit = m.group(2)
                    exp = m.group(3)
                    callput = m.group(4)
                    strike = round(float(m.group(5)) / 1000, 2)
                    symbol = f"{underlying}{optional_digit}_{exp}{callput}{strike}" #normalize option symbol
                    pos = Position(symbol, "OPTION", pos.longQuantity-pos.shortQuantity)
                    l.append(pos)
                except Exception as ex:
                    print (f"{pos.instrument.symbol} {ex}" )
            elif pos.instrument.assetType == 'EQUITY':
                pos = Position(pos.instrument.symbol, "STOCK", pos.longQuantity-pos.shortQuantity)
                l.append(pos)
        return l
    
    def send_request(self, url):
        response = requests.get(url, 
                                headers={'Authorization': "Bearer "+self.access_token})
        if response.status_code == 200:
            return DefaultMunch.fromDict(response.json())
        else:
            raise Exception(response.status_code)
        
        return response
 
    def schwab_option_symbol(self, symbol):
        m = re.compile(UNIFIED_OPTION_PATTERN).search(symbol)
        underlying = m.group(1)
        exp = m.group(2)
        callput = m.group(3)
        strike = int(float(m.group(4)) *1000)
        #schwab requires symbol = 'JD    240524C00032000'
        schwab_option_symbol = f"{underlying:<6}{exp}{callput}{strike :>08}"
        return schwab_option_symbol
      
    def get_positions(self) -> dict:
        self.get_account_number_hash_value()
        url =  f"{self.base_url}/trader/v1/accounts/{self.account_number}?fields=positions"
        response = self.send_request(url)
        time.sleep(0.5)
        self.pos_list =self.parse_positions(response.securitiesAccount.positions)
        self.pos_list.append(Position("SchWab","CASH",response.securitiesAccount.initialBalances.cashBalance))
        return self.pos_list
        
    def get_quote_obj(self, symbol, equity_type):
        if equity_type == 'OPTION':
            symbol = self.schwab_option_symbol(symbol)
        url=f"{self.base_url}/marketdata/v1/{symbol}/quotes?fields=quote"
        response = self.send_request(url)
        if response:
            quote_obj = response[symbol].quote
        else:
            quote_obj = None
        return quote_obj
            
    def get_chain_obj(self, option: Option):
        #https://api.schwabapi.com/marketdata/v1/chains?symbol=JD&contractType=CALL&strike=32&fromDate=2024-05-24&toDate=2024-05-24
        underlying = option.underlying
        exp =option.exp.strftime('%Y-%m-%d')
        callput = option.callput
        strike = f"{option.strike:g}"
        url=f"{self.base_url}/marketdata/v1/chains?symbol={underlying}&contractType={callput}&strike={strike}&fromDate={exp}&toDate={exp}"
        response = self.send_request(url)
        if response:
            chain_obj = response
        else:
            chain_obj = None
        return chain_obj
    
    def load_option_properties(self, pos):
        if pos.equity_type != 'OPTION':
            return pos
        option= Option(pos.symbol)
        if option.exp.date() < date.today():
            #Invalid option
            return None
        chain_obj = self.get_chain_obj(option)
        option.underlyingPrice = chain_obj.underlyingPrice
        if option.callput == 'CALL':
            option_data = chain_obj.callExpDateMap.values().__iter__().__next__().values().__iter__().__next__()[0]
        else:
            option_data = chain_obj.putExpDateMap.values().__iter__().__next__().values().__iter__().__next__()[0]
        option.price = round((option_data.ask + option_data.bid) / 2, 2)
        option.daysToExpiration = option_data.daysToExpiration
        option.intrinsic = max(option.underlyingPrice - option.strike,0) if option.callput == 'CALL' \
                                      else max(option.strike - option.underlyingPrice,0)
        option.extrinsic = option.price - option.intrinsic
        option.itm = 1 if option_data.inTheMoney == True else 0
        option.actionNeed = 1 if (option.itm == 1 and  option.daysToExpiration <= 5) or (option.extrinsic <= option.strike / 100) else 0
        option.daysToER = int((datetime.strptime(getERdate2(option.underlying), '%Y-%m-%d').date()-date.today()).days)
        option.delta = option_data.delta
        option.gamma = option_data.gamma
        option.theta = option_data.theta
        option.vega = option_data.vega
        option.openInterest = option_data.openInterest
        option.volatility = option_data.volatility
        
        pos.property = option
        
        return pos

class YieldMax(Exchange):
    def __init__(self):
        super().__init__()
        self.access_token = ""
        self.account_number = ""
        self.base_url = "https://api.schwabapi.com"
        self.option_pattern = r'([a-zA-Z]+)(\d*)\s+(\d+)([C|P])(\d+\.?\d*)'
        self.auth()
        
    def auth(self) -> str:
        headers = {
            "Authorization": f'Basic {base64.b64encode(f"{SCHWAB_APP_KEY}:{SCHWAB_APP_SECRET}".encode()).decode()}',
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "grant_type": "refresh_token",
            "refresh_token": SCHWAB_REFRESH_TOKEN,
        }    
        response = requests.post(
            url="https://api.schwabapi.com/v1/oauth/token",
            headers=headers,
            data=payload,
        )
        if response.status_code == 200:
            self.access_token = response.json()['access_token']
            return self.access_token
        else:
            return ""        
        
    def get_account_number_hash_value(self) -> str:
        url = f"{self.base_url}/trader/v1/accounts/accountNumbers"
        response = self.send_request(url)
        self.account_number = response[0].hashValue
        return self.account_number
    
    def parse_positions(self, positions):
        l = []
        for pos in positions:
            if pos.instrument.assetType == 'OPTION':
                try:
                    m = re.compile(self.option_pattern).search(pos.instrument.symbol)
                    underlying = m.group(1)
                    optional_digit = m.group(2)
                    exp = m.group(3)
                    callput = m.group(4)
                    strike = round(float(m.group(5)) / 1000, 2)
                    symbol = f"{underlying}{optional_digit}_{exp}{callput}{strike}" #normalize option symbol
                    pos = Position(symbol, "OPTION", pos.longQuantity-pos.shortQuantity)
                    l.append(pos)
                except Exception as ex:
                    print (f"{pos.instrument.symbol} {ex}" )
            elif pos.instrument.assetType == 'EQUITY':
                pos = Position(pos.instrument.symbol, "STOCK", pos.longQuantity-pos.shortQuantity)
                l.append(pos)
        return l
    
    def send_request(self, url):
        response = requests.get(url, 
                                headers={'Authorization': "Bearer "+self.access_token})
        if response.status_code == 200:
            return DefaultMunch.fromDict(response.json())
        else:
            raise Exception(response.status_code)
        
        return response
 
    def schwab_option_symbol(self, symbol):
        m = re.compile(UNIFIED_OPTION_PATTERN).search(symbol)
        underlying = m.group(1)
        exp = m.group(2)
        callput = m.group(3)
        strike = int(float(m.group(4)) *1000)
        #schwab requires symbol = 'JD    240524C00032000'
        schwab_option_symbol = f"{underlying:<6}{exp}{callput}{strike :>08}"
        return schwab_option_symbol
      
    def get_intraday_positions(self) -> dict:
        #load position folder
        folder =  os.path.dirname(os.path.realpath(__file__))+os.sep+"yieldmax"+os.sep
        filenames = [datetime.strptime(os.path.splitext(name)[0], '%Y-%m-%d') for name in os.listdir(folder)]
        latest_folder = sorted(filenames)[-1].strftime("%Y-%m-%d")        
        self.portf_files =  [name for name in os.listdir(folder+os.sep+latest_folder)
                             if not os.path.isdir(folder+os.sep+latest_folder+os.sep+name) and 
                             name == "Yieldmax_Intraday - intraday.csv"]
        self.portf_df = dict()
        for f in self.portf_files:
            self.portf_df[f] = pd.read_csv(folder+os.sep+latest_folder+os.sep+f)
        
        self.norm_df = {}
        for name, portf_df in  self.portf_df.items():
            pos_list = []
            for row in portf_df.itertuples(index=False):
                if all(pd.isna(value) for value in row):
                    continue
                if row.Type == 'B' or row.Type == 'BC':
                    direction = 1
                elif row.Type == 'SS' or row.Type == 'S':
                    direction = -1
                if re.compile(YIELDMAX_OPTION_PATTERN).search(row.Ticker):
                    pos = Position(row.Ticker, "OPTION", direction*row._4, 0, row.Fund)
                else:
                    pos = Position(row.Ticker, "STOCK", direction*row._4, 0, row.Fund)
                pos_list.append(pos)
            self.norm_df[name] = pos_list
        return self.norm_df
    
    def get_all_days_positions(self) -> pd.DataFrame():
        #load position folder
        df_total = pd.DataFrame()
        yieldmax_folder =  os.path.dirname(os.path.realpath(__file__))+os.sep+"yieldmax"+os.sep
        folders = [datetime.strptime(os.path.splitext(name)[0], '%Y-%m-%d').strftime("%Y-%m-%d") for name in os.listdir(yieldmax_folder)]
        #latest_folder = sorted(filenames)[-1].strftime("%Y-%m-%d")
        for folder in folders:
            self.portf_files =  [name for name in os.listdir(yieldmax_folder+os.sep+folder)
                                 if not os.path.isdir(yieldmax_folder+os.sep+folder+os.sep+name) and 
                                 name != "Yieldmax_Intraday - intraday.csv"]
            for f in self.portf_files:
                df = pd.read_csv(yieldmax_folder+os.sep+folder+os.sep+f)
                df_total = pd.concat([df_total, df])
        
        df_total.set_index(["Date", "Account"], inplace=True)
        return df_total
    
    def get_positions(self) -> dict:
        #load position folder
        folder =  os.path.dirname(os.path.realpath(__file__))+os.sep+"yieldmax"+os.sep
        filenames = [datetime.strptime(os.path.splitext(name)[0], '%Y-%m-%d') for name in os.listdir(folder)]
        latest_folder = sorted(filenames)[-1].strftime("%Y-%m-%d")        
        self.portf_files =  [name for name in os.listdir(folder+os.sep+latest_folder)
                             if not os.path.isdir(folder+os.sep+latest_folder+os.sep+name) and 
                             name != "Yieldmax_Intraday - intraday.csv"]
        self.portf_df = dict()
        for f in self.portf_files:
            self.portf_df[f] = pd.read_csv(folder+os.sep+latest_folder+os.sep+f)
            
        self.norm_df = {}
        for name, portf_df in  self.portf_df.items():
            pos_list = []
            for row in portf_df.itertuples():
                if row.StockTicker.startswith('912'):
                    pos = Position(row.StockTicker, "CASH", row.Shares,row.Weightings, row.Account)
                elif row.StockTicker.startswith('FGXXX'):
                    pos = Position(row.StockTicker, "CASH", row.Shares,row.Weightings, row.Account)
                elif row.StockTicker.startswith('Cash'):
                    pos = Position(row.StockTicker, "CASH", row.Shares,row.Weightings, row.Account)
                elif re.compile(YIELDMAX_OPTION_PATTERN).search(row.StockTicker):
                    pos = Position(row.StockTicker, "OPTION", row.Shares,row.Weightings, row.Account)
                else:
                    pos = Position(row.StockTicker, "STOCK", row.Shares,row.Weightings, row.Account)
                pos_list.append(pos)
            self.norm_df[name] = pos_list
        return self.norm_df
        
    def get_quote_obj(self, symbol, equity_type):
        if equity_type == 'OPTION':
            symbol = self.schwab_option_symbol(symbol)
        url=f"{self.base_url}/marketdata/v1/{symbol}/quotes?fields=quote"
        response = self.send_request(url)
        if response:
            quote_obj = response[symbol].quote
        else:
            quote_obj = None
        return quote_obj
            
    def get_chain_obj(self, option: Option):
        #https://api.schwabapi.com/marketdata/v1/chains?symbol=JD&contractType=CALL&strike=32&fromDate=2024-05-24&toDate=2024-05-24
        underlying = option.underlying
        exp =option.exp.strftime('%Y-%m-%d')
        callput = option.callput
        strike = f"{option.strike:g}"
        url=f"{self.base_url}/marketdata/v1/chains?symbol={underlying}&contractType={callput}&strike={strike}&fromDate={exp}&toDate={exp}"
        response = self.send_request(url)
        if response:
            chain_obj = response
        else:
            chain_obj = None
        return chain_obj
    
    def load_option_properties(self, pos):
        if pos.equity_type != 'OPTION':
            return pos
        option= Option(pos.symbol)
        chain_obj = self.get_chain_obj(option)
        option.underlyingPrice = chain_obj.underlyingPrice
        if option.callput == 'CALL':
            option_data = chain_obj.callExpDateMap.values().__iter__().__next__().values().__iter__().__next__()[0]
        else:
            option_data = chain_obj.putExpDateMap.values().__iter__().__next__().values().__iter__().__next__()[0]
        option.price = round((option_data.ask + option_data.bid) / 2, 2)
        option.daysToExpiration = option_data.daysToExpiration
        option.intrinsic = max(option.underlyingPrice - option.strike,0) if option.callput == 'CALL' \
                                      else max(option.strike - option.underlyingPrice,0)
        option.extrinsic = option.price - option.intrinsic
        option.itm = 1 if option_data.inTheMoney == True else 0
        option.actionNeed = 1 if (option.itm == 1 and  option.daysToExpiration <= 5) or (option.extrinsic <= option.strike / 100) else 0
        option.daysToER = int((datetime.strptime(getERdate2(option.underlying), '%Y-%m-%d').date()-date.today()).days)
        option.delta = option_data.delta
        option.gamma = option_data.gamma
        option.theta = option_data.theta
        option.vega = option_data.vega
        option.openInterest = option_data.openInterest
        option.volatility = option_data.volatility
        
        pos.property = option
        
        return pos
        
def build_option_table(portf: Portfolio, schwab:Schwab,sort_by_exp=True ) -> str:
    #Create options table
    rows = []
    for pos in  portf.portf_list:
        if pos.equity_type != 'OPTION':
            continue
        option = pos.property
        df_options = alldays_pos_df.loc[(alldays_pos_df['StockTicker']==pos.symbol) & (alldays_pos_df.index.get_level_values(1) == pos.etf)].reset_index().sort_values("Date")        
        row = {'Symbol':option.underlying,
                  'ETF': pos.etf,
                  'Weight': pos.weight,
                  'Weight (-1d)':df_options.iloc[-2]['Weightings'] if len(df_options) >= 2 and pos.weight != 0 else 0,
                  'Weight (-2d)':df_options.iloc[-3]['Weightings'] if len(df_options) >= 3 and pos.weight != 0 else 0,
                  'Weight (-5d)':df_options.iloc[-6]['Weightings'] if len(df_options) >= 6 and pos.weight != 0 else 0,                  
                  'ITM':option.itm,
                  'Price': option.price,
                  'DaysToExp': option.daysToExpiration,
                  'DaysToER':option.daysToER,
                  'Quantity': pos.quantity,
                  'Quantity (-1d)':df_options.iloc[-2]['Shares'] if len(df_options) >= 2 and pos.weight != 0 else 0,
                  'Quantity (-2d)':df_options.iloc[-3]['Shares'] if len(df_options) >= 3 and pos.weight != 0 else 0,
                  'Quantity (-5d)':df_options.iloc[-6]['Shares'] if len(df_options) >= 6 and pos.weight != 0 else 0,                       
                  'Extrinsic': option.extrinsic,
                  'CallPut': option.callput,
                  'Strike':option.strike,
                  'Underlying':option.underlyingPrice,
                  'Exp':option.exp,
                  'Delta': option.delta,
                  'Gamma': option.gamma,
                  'Theta': option.theta,
                  'Vega': option.vega,
                  'OpenInterest': option.openInterest,
                  'Volatility': option.volatility} 
        rows.append(row)
    if rows == []:
        return pd.DataFrame()
    df = pd.DataFrame.from_records(rows)
    #df['Unit'] = 100
    if sort_by_exp == True:
        df.sort_values(by=[ 'ETF','Symbol', 'DaysToExp'],inplace=True)
    else:
        df.sort_values(by=[ 'ETF','Symbol', 'DaysToExp'],inplace=True)
    df.style.highlight_between(left=1,right=1,subset=['ITM'],props="background:#FFFF00")\
    .highlight_between(left=0,right=5,subset=['DaysToExp'],props="background:#a1eafb")\
    .highlight_between(left=0,right=5,subset=['DaysToER'],props="background:#a1eafb")\
    .format(precision=2)
    df.style.set_table_styles([dict(selector="th",props=[('max-width', '20px')])])
    
    return df
    
def build_stock_table(portf):
    rows = []
    for pos in  portf.portf_list:
        if pos.equity_type != 'STOCK':
            continue
        row = {'Symbol':pos.symbol,
                   'ETF': pos.etf,
                   'Weight': pos.weight,
                  'Quantity': pos.quantity} 
        rows.append(row)
    if rows == []:
        return pd.DataFrame()        
    df = pd.DataFrame.from_records(rows)
    df['Delta'] = 1
    df['Unit'] = 1
    df['CallPut'] = 'STOCK'
    return df

def build_cash_table(portf):
    rows = []
    for pos in  portf.portf_list:
        if pos.equity_type != 'CASH':
            continue
        df_options = alldays_pos_df.loc[(alldays_pos_df['StockTicker']==pos.symbol) & (alldays_pos_df.index.get_level_values(1) == pos.etf)].reset_index().sort_values("Date")        
        row = {'Symbol':pos.symbol,
                  'ETF': pos.etf,
                  'Weight': pos.weight,
                  'Weight (-1d)':df_options.iloc[-2]['Weightings'] if len(df_options) >= 2 and pos.weight != 0 else 0,
                  'Weight (-2d)':df_options.iloc[-3]['Weightings'] if len(df_options) >= 3 and pos.weight != 0 else 0,
                  'Weight (-5d)':df_options.iloc[-6]['Weightings'] if len(df_options) >= 6 and pos.weight != 0 else 0,                         
                  'Quantity': pos.quantity} 
        rows.append(row)
    if rows == []:
        return pd.DataFrame()        
    df = pd.DataFrame.from_records(rows)
    return df

def concat_tables(option_df, esp, stock_df, cash_df):
    tables_html = """
        {0}
        {1}
        {2}
        {3}
    """.format(build_table(option_df, 'blue_light'),
               build_table(esp, 'blue_light'),
               build_table(stock_df, 'blue_light'),
               build_table(cash_df, 'blue_light'))
    return tables_html

def send_email_html(body_html):
    recipients = ['omnimahui@gmail.com']
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = "YieldMax portfolios"
    msg['From'] = 'omnimahui@gmail.com'
    
    
    html = """\
    <html>
      <head></head>
      <body>
        {0}
      </body>
    </html>
    """.format(body_html)
    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    
    smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
    smtp.ehlo()  # send the extended hello to our server
    smtp.starttls()  # tell server we want to communicate with TLS encryption
    smtp.login(SMTP_USER, SMTP_PASS)  # login to our email server
    
    # send our email message 'msg' to our boss
    smtp.sendmail(msg['From'],
                  emaillist,
                  msg.as_string())
    
    smtp.quit()  # finally, don't forget to close the connection
    return

def send_email(option_df, esp, stock_df, cash_df):
    recipients = ['omnimahui@gmail.com']
    emaillist = [elem.strip().split(',') for elem in recipients]
    msg = MIMEMultipart()
    msg['Subject'] = "Option Portfolio"
    msg['From'] = 'omnimahui@gmail.com'
    
    
    html = """\
    <html>
      <head></head>
      <body>
        {0}
        {1}
        {2}
        {3}
      </body>
    </html>
    """.format(build_table(option_df, 'blue_light'),
               build_table(esp, 'blue_light'),
               build_table(stock_df, 'blue_light'),
               build_table(cash_df, 'blue_light'))
    part1 = MIMEText(html, 'html')
    msg.attach(part1)
    
    smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
    smtp.ehlo()  # send the extended hello to our server
    smtp.starttls()  # tell server we want to communicate with TLS encryption
    smtp.login(SMTP_USER, SMTP_PASS)  # login to our email server
    
    # send our email message 'msg' to our boss
    smtp.sendmail(msg['From'],
                  emaillist,
                  msg.as_string())
    
    smtp.quit()  # finally, don't forget to close the connection
    return

def esp(group_df):
    #print (group_df['symbol'])
    #print (group_df['delta']*100*grouCorniche de la plage d'Agadirp_df['quantity'])
    d = {}
    d['Delta'] = round((group_df['Delta']*group_df['Quantity']).sum(),4)
    d['Gamma'] = (group_df['Gamma']*group_df['Quantity']).sum()
    d['Vega'] = (group_df['Vega']*group_df['Quantity']).sum()
    d['Theta'] = round((group_df['Theta']*group_df['Quantity']).sum(),4)
    d['Covercall_capability'] = group_df.loc[group_df['CallPut']== 'CALL']['Quantity'].sum() + math.floor(group_df.loc[group_df['CallPut']== 'STOCK']['Quantity'].sum()/100)
    return pd.Series(d, index=['Delta', 'Gamma', 'Vega', 'Theta','Covercall_capability'])

yieldmax = YieldMax()
schwab =  Schwab()
intraday_ym=yieldmax.get_intraday_positions()
latest_pos_ym=yieldmax.get_positions()
alldays_pos_df = yieldmax.get_all_days_positions()

body_html = ""
for name, positions in  intraday_ym.items():
    portf = Portfolio()
    for pos in  positions:
        try:
            pos = schwab.load_option_properties(pos)
            if pos != None:
                portf.add(pos)

        except Exception as ex:
            print (f"{pos.symbol} exception: {ex}")

    option_df = build_option_table(portf, schwab, sort_by_exp=False)
    stock_df = build_stock_table(portf)
    cash_df = build_cash_table(portf)
    #total_df=pd.concat([option_df, stock_df], join="outer").fillna(0)
    esp_df = pd.DataFrame()
    if not option_df.empty:
        esp_df=option_df.groupby(['Symbol', 'ETF']).apply(esp)
        esp_df.reset_index(inplace=True)

    body_html += concat_tables(esp_df, option_df, stock_df, cash_df)
time.sleep(5)
for name, positions in  latest_pos_ym.items():
    portf = Portfolio()
    for pos in  positions:
        try:
            pos = schwab.load_option_properties(pos)
            if pos != None:
                portf.add(pos)
        except Exception as ex:
            print (f"{pos.symbol} exception: {ex}")

    option_df = build_option_table(portf, schwab)
    stock_df = build_stock_table(portf)
    cash_df = build_cash_table(portf)
    #total_df=pd.concat([option_df, stock_df], join="outer").fillna(0)
    esp_df = pd.DataFrame()
    if not option_df.empty:
        esp_df=option_df.groupby(['Symbol', 'ETF']).apply(esp)
        esp_df.reset_index(inplace=True)

    body_html += concat_tables(esp_df, option_df, stock_df, cash_df)

send_email_html(body_html)
