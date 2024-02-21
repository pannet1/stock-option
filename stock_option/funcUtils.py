import math
import pandas as pd
import datetime as dt
from datetime import date , timedelta
from collections import OrderedDict 
import itertools
class helper_funcs :
    def __init__(self):  
        self.data = pd.read_csv('https://developers.stocknote.com/doc/ScripMaster.csv')
        self.clean_data()
        self.date_set = []
        
    def clean_data(self) :
        today = date.today().strftime("%Y-%m-%d")
        self.data.drop(self.data[(self.data.exchange == "CDS") | (self.data.exchange == "BSE")].index, inplace=True)
        self.data.drop(self.data[(self.data.expiryDate < today) & (self.data.expiryDate.isnull() == False)].index,inplace=True)
    
    
    def resample_data(self,data,period,offset=None,fill=False,exchange="NSE") :
        dict1 = OrderedDict([('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        data = data.set_index('dateTime')
        data = data.resample(period , offset=offset).agg(dict1)
        data.reset_index(inplace = True)
        # if fill :
        #     data.fillna(method='ffill',inplace=True)
        #     data = data[data['dateTime'].dt.strftime("%Y-%m-%d").isin(self.date_set)]
        #     data.set_index('dateTime',inplace=True)
        #     if exchange == "NSE" or exchange == "NFO" :
        #         data = data.between_time('9:15', '15:30',include_end=False)
        #     elif exchange == "MCX" :
        #         data = data.between_time('9:00', '23:30',include_end=False)
        #     data.reset_index(inplace = True)        
        data.dropna(inplace=True)
        data.reset_index(inplace = True,drop=True)
        return data
    
    
    def expiry_list(self,symbol="NIFTY"):
        EX_LIST = list(set(self.data[(self.data.instrument == "OPTIDX") & (self.data.name == symbol)].expiryDate.tolist()))
        EX_LIST.sort()
        return EX_LIST
    

    def NFO_find_month_expiry(self,nearest=0,symbol="NIFTY"):
        
        ex_list = self.expiry_list(symbol)
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
    
    def NFO_find_expiry(self,week=0,symbol="NIFTY") :
        
        ex_list = self.expiry_list(symbol)
        return ex_list[week]


class zerodha_get_inst :
    def __init__(self,NSE_data=None,NFO_data=None,MCX_data=None):
        self.NSE_data = NSE_data
        if isinstance(NFO_data ,pd.DataFrame) :
            NFO_data['expiry'] = pd.to_datetime(NFO_data['expiry']).dt.strftime('%Y-%m-%d')
            
        self.NFO_data = NFO_data
        self.MCX_data = MCX_data
    
    def __get_stock_token_details(self,genralStockName) :
        if genralStockName == "BANKNIFTY" :
            stockName = "NIFTY BANK"
        elif genralStockName == "NIFTY" :
            stockName = "NIFTY 50"
        else :
            stockName = genralStockName
                
        try :
            if self.NSE_data is None :
                raise Exception("No NSE data available")
            selRow = self.NSE_data[(self.NSE_data['tradingsymbol']==stockName)]
            inst_token = int(selRow['instrument_token'].iloc[0])
            tradingsymbol = (selRow['tradingsymbol'].iloc[0])
            return inst_token , tradingsymbol
        except Exception as e:
            print("Error getting stock token details {} {}".format(stockName,e))
            return None , None

    def __get_fut_token_details(self,futName,expiry) :
        try :
            if self.NFO_data is None :
                raise Exception("No NFO data available")
            try :
                dt.datetime.strptime(expiry, "%Y-%m-%d")
            except :
                raise TypeError("Expiry must be of str format of %Y-%m-%d")
            selRow = self.NFO_data[(self.NFO_data['name']==futName) & (self.NFO_data['expiry'] == expiry) & (self.NFO_data['segment'] == "NFO-FUT")]

            inst_token = int(selRow['instrument_token'].iloc[0])
            tradingsymbol = (selRow['tradingsymbol'].iloc[0])
            return inst_token , tradingsymbol
        except Exception as e:
            print("Error getting fut token details {} {}".format(futName,e))
            return None , None
            
    def __get_opt_token_details(self,spotName,expiry,strike,opType) :
        try :
            if self.NFO_data is None :
                raise Exception("No NFO data available")
            try :
                dt.datetime.strptime(expiry, "%Y-%m-%d")
            except :
                raise TypeError("Expiry must be of str format of %Y-%m-%d")
            selRow = self.NFO_data[(self.NFO_data['name']==spotName) & (self.NFO_data['expiry'] == expiry) & (self.NFO_data['strike'] == strike) & (self.NFO_data['instrument_type'] == opType)]
            inst_token = int(selRow['instrument_token'].iloc[0])
            tradingsymbol = (selRow['tradingsymbol'].iloc[0])
            return inst_token , tradingsymbol
        except Exception as e:
            print("Error getting opt token details {} {} {} {} {}".format(spotName,expiry,strike,opType,e))
            return None , None
    
    def __get_individual_inst_token_details(self, instrument) :
        if not isinstance(instrument,tuple) :
            print("Instrument not of type tuple {}".format(instrument))
            return None

        if instrument[2] == "None" :
            if instrument[1] == "None" :
                inst_token , tradingsymbol = self.__get_stock_token_details(instrument[0])
            else :
                inst_token , tradingsymbol = self.__get_fut_token_details(instrument[0],instrument[1])
        else :
            
            inst_token , tradingsymbol = self.__get_opt_token_details(instrument[0],instrument[1],instrument[2],instrument[3])

        return inst_token , tradingsymbol

    def NFO_find_expiry(self,week=0,symbol="NIFTY") :
        
        ex_list = self.get_expiry_list(symbol)
        return ex_list[week]
     
    def get_expiry_list(self,symbol="NIFTY"):
        EX_LIST = self.NFO_data[(self.NFO_data['name']==symbol) & (self.NFO_data['segment'] == "NFO-OPT")]['expiry'].unique()
        EX_LIST.sort()
        return EX_LIST
    def get_strike_list_by_expiry(self,symbol,expiry) :
        if self.NFO_data is None :
            raise Exception("No NFO data available")
        strike_list = self.NFO_data[(self.NFO_data['name']==symbol)&(self.NFO_data['segment'] == "NFO-OPT")& (self.NFO_data['expiry'] == expiry)]['strike'].unique()
        strike_list.sort()
        strike_list = [float(strike) for strike in strike_list]
        return strike_list
    
    def get_strike_list(self,symbol) :
        if self.NFO_data is None :
            raise Exception("No NFO data available")
        strike_list = self.NFO_data[(self.NFO_data['name']==symbol)&(self.NFO_data['segment'] == "NFO-OPT")]['strike'].unique()
        strike_list.sort()
        strike_list = [float(strike) for strike in strike_list]
        return strike_list
    
    def get_token_list(self,data):
        return list(data.values())
    
    def get_strike_diff(self,symbol) :
        strike_list = self.get_strike_list(symbol)
        strike_list = [int(strike*100) for strike in strike_list]
        length = len(strike_list)
        mid = int(length/2)
        start = max(0, mid-2)
        end = min(length-1, mid+2)
        gcd = strike_list[start]
        for i in range(start, end):
            gcd = math.gcd(gcd, strike_list[i])
        gcd = (gcd / 100)
        if int(gcd) == gcd :
            gcd = int(gcd)
        return gcd       
    
    def set_global_data(self,exchange,data) :
        if exchange not in ["NSE","NFO","MCX"] :
            return {"status":"error","message":"Exchange is not valid"}
        elif exchange == "NSE" :
            self.NSE_data = data
        elif exchange == "NFO" :
            self.NFO_data = data
        elif exchange == "MCX" :
            self.MCX_data = data
            
        return {"status":"success","message":"Data stored in {} exchange".format(exchange)}
        
    def get_token_details(self,instrument):
        failureList = []
        successList = []
        InstrumentTokenMapper = {}
        InstrumentTradingSymbolMapper = {}
        if isinstance(instrument,list) :
            for _iter in instrument :
                inst_token , tradingSymbol = self.__get_individual_inst_token_details(_iter)
                if inst_token is None :
                    failureList.append(_iter)
                else :
                    successList.append(_iter)
                    InstrumentTokenMapper[tuple(_iter)] = inst_token
                    InstrumentTradingSymbolMapper[tuple(_iter)] = tradingSymbol

        else:
            inst_token , tradingSymbol = self.__get_individual_inst_token_details(instrument)
            if inst_token == None :
                failureList.append(instrument)
            else :
                successList.append(instrument)
                InstrumentTokenMapper[tuple(instrument)] = inst_token
                InstrumentTradingSymbolMapper[tuple(instrument)] = tradingSymbol

        
        return InstrumentTokenMapper,InstrumentTradingSymbolMapper ,successList, failureList     

    def get_allComb_option_tokenDetails(self,spotName,expiry,strike,opType="All"):
        if not isinstance(spotName,list) :
            spotName = [spotName]
        
        if not isinstance(expiry,list) :
            expiry = [expiry]
        
        if not isinstance(strike,list) :
            strike = [strike]
        
        if not isinstance(opType,list) :
            if opType == "All" :
                opType = ["CE","PE"]
            else :
                opType = [opType]
        
        
        
        requiredInstList = (list(itertools.product(*[spotName,expiry,strike,opType])))

        return self.get_token_details(requiredInstList)
            
        
    def get_option_instrument_list(self,price,spotName,deviation,expiry) :

        
        strike_diff = self.get_strike_diff(spotName)
        ATM = round(price/strike_diff) * strike_diff
        upper_strike = ATM + strike_diff*deviation
        lower_strike = ATM - strike_diff*deviation
        
        INST_LIST = []
        
        strike = lower_strike
        
        while strike <= upper_strike :

            INST_LIST.extend([tuple([spotName,expiry,strike,"CE"]),tuple([spotName,expiry,strike,"PE"])])
            strike = strike + strike_diff
        
        return self.get_token_details(INST_LIST)