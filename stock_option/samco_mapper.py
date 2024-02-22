import pandas as pd
import getpass
from datetime import date , timedelta
from collections import OrderedDict 

class samco_data:  
      
    # init method or constructor   
    def __init__(self):  

        # self.data = pd.read_csv('https://developers.stocknote.com/doc/ScripMaster.csv')
        # self.clean_data()
        self.date_set = []
        
    def clean_data(self) :
        today = date.today().strftime("%Y-%m-%d")
        self.data.drop(self.data[(self.data.exchange == "CDS") | (self.data.exchange == "BSE")].index, inplace=True)
        self.data.drop(self.data[(self.data.expiryDate < today) & (self.data.expiryDate.isnull() == False)].index,inplace=True)
    
    def format_data(self,data,trim_date=True,exchange="NSE") :
        data["high"] = pd.to_numeric(data["high"] , downcast = "float")
        data["low"] = pd.to_numeric(data["low"] , downcast = "float")
        data["open"] = pd.to_numeric(data["open"] , downcast = "float")
        data["close"] = pd.to_numeric(data["close"] , downcast = "float")
        data["volume"] = pd.to_numeric(data["volume"] , downcast = "float")
        if trim_date :
            data["dateTime"]= data["dateTime"].str.slice(0, -2, 1) 
        data["dateTime"] = pd.to_datetime(data['dateTime'])
        data.set_index('dateTime',inplace=True)
        if trim_date :
            if exchange == "NSE" or exchange == "NFO" :
                data = data.between_time('9:15', '15:30',include_end=False)
            elif exchange == "MCX" :
                data = data.between_time('9:00', '23:30',include_end=False)
        data.reset_index(inplace = True)
        return data

    def resample_data(self,data,period,offset=None,fill=False,exchange="NSE") :
        dict1 = OrderedDict([('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        data = data.set_index('dateTime')
        data = data.resample(period , offset=offset).agg(dict1)
        data.reset_index(inplace = True)
        if fill :
            data.fillna(method='ffill',inplace=True)
            data = data[data['dateTime'].dt.strftime("%Y-%m-%d").isin(self.date_set)]
            data.set_index('dateTime',inplace=True)
            if exchange == "NSE" or exchange == "NFO" :
                data = data.between_time('9:15', '15:30',include_end=False)
            elif exchange == "MCX" :
                data = data.between_time('9:00', '23:30',include_end=False)
            data.reset_index(inplace = True)        
        data.dropna(inplace=True)
        data.reset_index(inplace = True,drop=True)
        return data
        
    def set_date_list(self,samco,days,symbol="RELIANCE"):
        stk_start_date = date.today() - timedelta(days)
        stk_end_date = date.today()


        data = samco.get_historical_candle_data(symbol_name=symbol,exchange=samco.EXCHANGE_NSE, from_date=stk_start_date,to_date=stk_end_date)
        data = pd.DataFrame(data['historicalCandleData'])
        date_set1 = data.date.values.tolist()
        date_set1.append(date.today().strftime("%Y-%m-%d"))
        self.date_set = date_set1

    def expiry_list(self,INS="NIFTY"):
        EX_LIST = list(set(self.data[(self.data.instrument == "OPTIDX")&(self.data.name == INS)].expiryDate.tolist()))
        EX_LIST.sort()
        return EX_LIST
    
    
    def NFO_find_month_expiry(self,nearest=0,INS="NIFTY"):
        
        ex_list = self.expiry_list(INS)
        count = 0
        for i in range(len(ex_list)) :
            if ex_list[i][5:7] != ex_list[i+1][5:7] :
                if nearest == count :
                    return ex_list[i]
                else :
                    count = count + 1
                    continue
    
    
    def MCX_expiry_list(self,symbol):

        EX_LIST = list(set(self.data[(self.data.exchange == "MFO") & (self.data.name == symbol) & (self.data.instrument.str.contains('FUT'))].expiryDate.tolist()))
        EX_LIST.sort()
        return EX_LIST
    
    def NFO_find_expiry(self,week=0,INS="NIFTY") :
        
        ex_list = self.expiry_list(INS)
        print(ex_list)
        return ex_list[week]
    
    def find_strike_diff(self,symbol) :

        STR_LIST = list(set(self.data[(self.data.exchange == "NFO") & (self.data.name == symbol) & (self.data.instrument.str.contains('OPT')) & (self.data.tradingSymbol.str.contains('CE'))].strikePrice.tolist()))
        STR_LIST = list(set(STR_LIST))
        STR_LIST.sort()
        STR_DIFF = []
        for i in range(1,len(STR_LIST)) :
            STR_DIFF.append(STR_LIST[i]-STR_LIST[i-1])

        return min(STR_DIFF)

    def get_strike_list(self,symbol,lower_limit,upper_limit) :

        STR_LIST = self.data[(self.data.name == symbol) & (self.data.instrument.str.contains('OPT')) & (self.data.strikePrice <= upper_limit) & (self.data.strikePrice >= lower_limit) & (self.data.tradingSymbol.str.contains('CE'))].strikePrice.tolist()
        STR_LIST = list(set(STR_LIST))
        STR_LIST.sort()

        return STR_LIST

    def MCX_find_expiry(self,symbol,nearest=0):
        return self.MCX_expiry_list(symbol)[nearest]
    
    def get_instrument_by_symbol(self,symbol) :
        
        return self.data[(self.data.exchange == "NSE") & (self.data.name == symbol)].name.values.tolist()[0]
    
        
    def get_instrument_for_fno(self,symbol,expiry_date,is_fut=True,strike=None,is_CE=True) :
        
        assert type(is_fut) == bool , "Enter True or False for is_fut in bool format"
        assert type(is_CE) == bool , "Enter True or False for is_CE in bool format"
        
        if is_fut :
            
            return self.data[(self.data.name == symbol) & (self.data.expiryDate == expiry_date) & (self.data.instrument.str.contains('FUT'))].tradingSymbol.values.tolist()[0]
        
        else :
            
            if is_CE :
                
                return self.data[(self.data.name == symbol) & (self.data.expiryDate == expiry_date) & (self.data.instrument.str.contains('OPT')) & (self.data.strikePrice == strike) & (self.data.tradingSymbol.str.contains('CE'))].tradingSymbol.values.tolist()[0]
            
            else:
                
                return self.data[(self.data.name == symbol) & (self.data.expiryDate == expiry_date) & (self.data.instrument.str.contains('OPT')) & (self.data.strikePrice == strike) & (self.data.tradingSymbol.str.contains('PE'))].tradingSymbol.values.tolist()[0]
    
    def get_CE_data(self,symbol,expiry_date,lower_limit,upper_limit) :

        return self.data[(self.data.name == symbol) & (self.data.expiryDate == expiry_date) & (self.data.instrument.str.contains('OPT')) & (self.data.strikePrice <= upper_limit) & (self.data.strikePrice >= lower_limit) & (self.data.tradingSymbol.str.contains('CE'))]
            
    def get_PE_data(self,symbol,expiry_date,lower_limit,upper_limit) :

        return self.data[(self.data.name == symbol) & (self.data.expiryDate == expiry_date) & (self.data.instrument.str.contains('OPT')) & (self.data.strikePrice <= upper_limit) & (self.data.strikePrice >= lower_limit) & (self.data.tradingSymbol.str.contains('PE'))]
            
