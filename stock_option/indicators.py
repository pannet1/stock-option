import numpy as np
import pandas as pd
import math as m
from collections import OrderedDict
from datetime import datetime
class indicators():
    
    def SMA(self,data,periods=14):
        res = data.rolling(periods).mean()
        return res

    def EMA(self,data,periods=14):
        res = data.ewm(span=periods,adjust=False).mean()
        return res

    def RMA(self,data,periods=14):
        res = data.ewm(alpha=1/periods,adjust=False).mean()
        return res

    def WMA(self,data,periods=14):
        weights = np.array([i+1 for i in range(periods)])
        sum_weights = np.sum(weights)
        res = (data.rolling(window=periods).apply(lambda x: np.sum(weights*x) / sum_weights, raw=False))
        return res

    def HMA(self,data,periods=14):
        res = self.WMA(2*self.WMA(data, int(periods/2))
              -self.WMA(data, periods), round(m.sqrt(periods)))
        return res

    def SMMA(self,data,periods = 14) :
        result = []
        sum1 = 0
        for index,value in data.iteritems():
            if index < periods - 1 :
                sum1 = sum1 + value
                result.append(np.NaN)
            elif index == periods - 1 :
                sum1 = sum1 + value
                sum1 = sum1/periods
                result.append(sum1)
            else :
                temp = (result[-1]*(periods-1) + value)/periods
                result.append(temp)
        
        return pd.Series(result)            

    def RSI(self,data,periods=14):
    
        delta = data.diff()
        dUp, dDown = delta.copy(), delta.copy()
        dUp[dUp < 0] = 0
        dDown[dDown > 0] = 0

        RolUp = self.RMA(dUp,periods)
        RolDown = self.RMA(dDown,periods).abs()

        RS = RolUp / RolDown
        
        
        rsi= 100.0 - (100.0 / (1.0 + RS))
        return rsi


    def ATR_all(self,data,periods=14):
        assert isinstance(data,pd.DataFrame) , "Datatype of data should be pandas Dataframe "
        data.columns = data.columns.str.lower()
        assert set(['high','low','close']).issubset(set(data.columns.tolist())) , "Dataframe didn't consist either of high , low or close"
        
        data['tr0'] = abs(data.high - data.low)
        data['tr1'] = abs(data.high - data.close.shift())
        data['tr2'] = abs(data.low - data.close.shift())
        tr = data[['tr0', 'tr1', 'tr2']].max(axis=1)
        tr[0] = np.NaN
        res = self.RMA(tr,periods=periods)
        return tr,res

    def ATR(self,data,periods=14):
        _ , res = self.ATR_all(data,periods)
        return res

    def TR(self,data,periods=14):
        res , _ = self.ATR_all(data,periods)
        return res

    def SUPERTREND(self,data,periods=14,multiplier=3):
        data = data.copy()
        assert isinstance(data,pd.DataFrame) , "Datatype of data should be pandas Dataframe "
        data.columns = data.columns.str.lower()
        assert set(['high','low','close']).issubset(set(data.columns.tolist())) , "Dataframe didn't consist either of high , low or close"

        """
        SuperTrend Algorithm :
        
            BASIC UPPERBAND = (HIGH + LOW) / 2 + Multiplier * ATR
            BASIC LOWERBAND = (HIGH + LOW) / 2 - Multiplier * ATR
            
            FINAL UPPERBAND = IF( (Current BASICUPPERBAND < Previous FINAL UPPERBAND) or (Previous Close > Previous FINAL UPPERBAND))
                                THEN (Current BASIC UPPERBAND) ELSE Previous FINALUPPERBAND)
            FINAL LOWERBAND = IF( (Current BASIC LOWERBAND > Previous FINAL LOWERBAND) or (Previous Close < Previous FINAL LOWERBAND)) 
                                THEN (Current BASIC LOWERBAND) ELSE Previous FINAL LOWERBAND)
            
            SUPERTREND = IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close <= Current FINAL UPPERBAND)) THEN
                            Current FINAL UPPERBAND
                        ELSE
                            IF((Previous SUPERTREND = Previous FINAL UPPERBAND) and (Current Close > Current FINAL UPPERBAND)) THEN
                                Current FINAL LOWERBAND
                            ELSE
                                IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close >= Current FINAL LOWERBAND)) THEN
                                    Current FINAL LOWERBAND
                                ELSE
                                    IF((Previous SUPERTREND = Previous FINAL LOWERBAND) and (Current Close < Current FINAL LOWERBAND)) THEN
                                        Current FINAL UPPERBAND
        """
        
        atr = 'ATR_' + str(periods)
        st = 'ST_' + str(periods) + '_' + str(multiplier)
        stx = 'STX_' + str(periods) + '_' + str(multiplier)
        ohlc = ['open','high','low','close']

        data['hl2'] = (data.high + data.low)/2
        data[atr] = self.ATR(data,periods)


        # Compute basic upper and lower bands
        data['basic_ub'] = data['hl2'] + multiplier * data[atr]
        data['basic_lb'] = data['hl2'] - multiplier * data[atr]
        
        # Compute final upper and lower bands
        data['final_ub'] = 0.00
        data['final_lb'] = 0.00
        for i in range(periods, len(data)):
            data['final_ub'].iat[i] = data['basic_ub'].iat[i] if data['basic_ub'].iat[i] < data['final_ub'].iat[i - 1] or data[ohlc[3]].iat[i - 1] > data['final_ub'].iat[i - 1] else data['final_ub'].iat[i - 1]
            data['final_lb'].iat[i] = data['basic_lb'].iat[i] if data['basic_lb'].iat[i] > data['final_lb'].iat[i - 1] or data[ohlc[3]].iat[i - 1] < data['final_lb'].iat[i - 1] else data['final_lb'].iat[i - 1]
        
        # Set the Supertrend value
        data[st] = 0.00
        
        for i in range(periods, len(data)):
            data[st].iat[i] = data['final_ub'].iat[i] if data[st].iat[i - 1] == data['final_ub'].iat[i - 1] and data[ohlc[3]].iat[i] <= data['final_ub'].iat[i] else \
                            data['final_lb'].iat[i] if data[st].iat[i - 1] == data['final_ub'].iat[i - 1] and data[ohlc[3]].iat[i] >  data['final_ub'].iat[i] else \
                            data['final_lb'].iat[i] if data[st].iat[i - 1] == data['final_lb'].iat[i - 1] and data[ohlc[3]].iat[i] >= data['final_lb'].iat[i] else \
                            data['final_ub'].iat[i] if data[st].iat[i - 1] == data['final_lb'].iat[i - 1] and data[ohlc[3]].iat[i] <  data['final_lb'].iat[i] else 0.00 
            
        
        # Mark the trend direction up/down
        data[stx] = np.where((data[st] > 0.00), np.where((data[ohlc[3]] < data[st]), 'down',  'up'), np.NaN)
        # Remove basic and final bands from the columns
        data.drop(['basic_ub', 'basic_lb', 'final_ub', 'final_lb'], inplace=True, axis=1)
        #data.fillna(0, inplace=True)
        x = data[st].copy()
        y = data[stx].copy()
        
        return x,y

    def MACD(self,data, fastEMA=12, slowEMA=26, signal=9):

        # Compute fast and slow EMA    
        fast_ema = self.EMA(data, fastEMA)
        slow_ema = self.EMA(data, slowEMA)
        # Compute MACD
        macd = fast_ema - slow_ema
        # Compute MACD Signal
        macd_signal = self.EMA(macd,signal)
        # Compute MACD Histogram
        hist = macd - macd_signal
        return macd,macd_signal,hist

    def BOLLINGER_BAND(self,data, periods=20, multiplier=2):
    
        sma = data.rolling(window=periods).mean()
        sd = data.rolling(window=periods).std()
        UPPER_BAND = sma + (multiplier * sd)
        LOWER_BAND = sma - (multiplier * sd)
        return UPPER_BAND,sma,LOWER_BAND
    
    def ADX(self,df,di_len=14,adx_len=14):
        df['plus_DM']=df.high-df.high.shift(1)
        df['minus_DM']=df.low.shift(1)-df.low
        df.plus_DM[df.plus_DM<0]=0
        df.minus_DM[df.minus_DM<0]=0
        df.plus_DM[df.plus_DM<df.minus_DM]=0
        df.minus_DM[df.minus_DM<df.plus_DM]=0
        df['H-L']=abs(df['high']-df['low'])
        df['H-PC']=abs(df['high']-df['close'].shift(1))
        df['L-PC']=abs(df['low']-df['close'].shift(1))
        df['TR']=df[['H-L','H-PC','L-PC']].max(axis=1)
        df['ATR'] = df['TR'].rolling(di_len).mean()#rolling mean of TR is ATR
        df['plus_DI'] = df['plus_DM'].ewm(alpha=1/di_len, adjust=False).mean()
        df['minus_DI'] = df['minus_DM'].ewm(alpha=1/di_len, adjust=False).mean()
        df['plus_DI'] = (df['plus_DI']/df['ATR'])*100
        df['minus_DI'] = (df['minus_DI']/df['ATR'])*100
        df['ADX'] = abs( df['plus_DI'] - df['minus_DI'] ) * 100 / ( df['plus_DI'] + df['minus_DI'] )
        df['SADX'] = df['ADX'].ewm(alpha=1/adx_len, adjust=False).mean()
        print(df.head(2))
        print(df.tail(15))

        return df['SADX'].copy()
    
    def vwap1(self,df):
        q = df['volume']#.values
        p = df['close']#.values
        return df.assign(vwap=np.where(q.cumsum()==0,np.NaN,(p * q).cumsum() / q.cumsum()))

    def vwap(self,df) :
        
        df['date'] = pd.to_datetime(df['date'])
        df = df.set_index('date')
        val =  df.groupby(df.index.date, group_keys=False).apply(self.vwap1)
        val.reset_index(drop=True, inplace=True)
        val = val["vwap"]
        # print(val)
        return val

    

    def Stoch_len(self,df):
        
        df["stoch"] = np.where((df["max1"] - df["min1"])==0,np.NaN,(df["feature"] - df["min1"])*100 / (df["max1"] - df["min1"]))
        return df


    def StochRSI(self,df,K,D,length):
        df["feature"] = self.RSI(df.close,length)
        df["min1"] = df.feature.rolling(length).min()
        df["max1"] = df.feature.rolling(length).max()
        df = self.Stoch_len(df)
        df["K"] = self.SMA(df.stoch,K)
        df["D"] = self.SMA(df.K,D)
        return df["K"] , df["D"]
    
    def Stoch(self,df,K,D,length):
        df["feature"] = df.close
        df["min1"] = df.low.rolling(length).min()
        df["max1"] = df.high.rolling(length).max()
        df = self.Stoch_len(df)
        df["K"] = self.SMA(df.stoch,K)
        df["D"] = self.SMA(df.K,D)
        return df["K"] , df["D"]

    def ORB(self,data,start_date,end_date) :
        mask = (data['date'] >= datetime.strptime(start_date, "%d-%m-%Y %H:%M:%S")) & (data['date'] <= datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S"))
        data['date'] = data['date'].dt.strftime("%d-%m-%Y %H:%M:%S")
        data = data.loc[mask]
        data.reset_index(drop = True , inplace = True)
        
        return data['open'].iat[0] , data['high'].max() , data['low'].min() , data['close'].iat[-1] , datetime.strptime(end_date, "%d-%m-%Y %H:%M:%S")       

    def CPR(self,df,his,req=["Pivot","TC","BC","R1","S1","R2","S2","R3","S3","R4","S4","R5","S5"]) :
        
        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])

        val = his.copy()
        #val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        val["close"] = val.close.shift(1)
        val["Pivot"] = (val["high"] + val["low"] + val["close"])/3
        val["IBC"] = (val["high"] + val["low"])/2
        val["ITC"] = 2*val["Pivot"] - val["IBC"]
        val["BC"] = val[['IBC','ITC']].min(axis=1)
        val["TC"] = val[['IBC','ITC']].max(axis=1)
        val["R1"] = 2*val["Pivot"] - val["low"]
        val["S1"] = 2*val["Pivot"] - val["high"]
        val["R2"] = val["Pivot"] + (val["high"] - val["low"])
        val["S2"] = val["Pivot"] - (val["high"] - val["low"])
        val["R3"] = val["high"] + 2 * (val["Pivot"] - val["low"])
        val["S3"] = val["low"] - 2 * (val["high"] - val["Pivot"])
        val["R4"] = val["high"] + 3 * (val["Pivot"] - val["low"])
        val["S4"] = val["low"] - 3 * (val["high"] - val["Pivot"])
        val["R5"] = val["high"] + 4 * (val["Pivot"] - val["low"])
        val["S5"] = val["low"] - 4 * (val["high"] - val["Pivot"])        
        df.reset_index(inplace=True)
        
        result = pd.merge(df,val[['date_only']+req],on='date_only',how="left")
        result.drop('date_only',axis='columns', inplace=True)

        
        return result[req]
    def Classic_CPR(self,df,his,req=["Pivot","R1","S1","R2","S2","R3","S3","R4","S4"]) :

        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])

        val = his.copy()
        #val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        val["close"] = val.close.shift(1)
        val["Pivot"] = (val["high"] + val["low"] + val["close"])/3
        val["R1"] = 2*val["Pivot"] - val["low"]
        val["S1"] = 2*val["Pivot"] - val["high"]
        val["R2"] = val["Pivot"] + (val["high"] - val["low"])
        val["S2"] = val["Pivot"] - (val["high"] - val["low"])
        val["R3"] = val["Pivot"] + 2 * (val["high"] - val["low"])
        val["S3"] = val["Pivot"] - 2 * (val["high"] - val["low"])
        val["R4"] = val["Pivot"] + 3 * (val["high"] - val["low"])
        val["S4"] = val["Pivot"] - 3 * (val["high"] - val["low"])
      
        df.reset_index(inplace=True)
        
        result = pd.merge(df,val[['date_only']+req],on='date_only',how="left")
        result.drop('date_only',axis='columns', inplace=True)

        
        return result[req]

    def Demark_CPR(self,df,his,req=["Pivot","R1","S1"]) :

        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])

        val = his.copy()
        #val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        val["close"] = val.close.shift(1)
        # val["open"] = val.open 
        val["Pivots"] = np.where((val["open"] >= val["close"]), (val["close"] + (val["high"]*2) + val["low"]),  (val["high"] + (val["low"]*2) + val["close"]))
        val["Pivot"]=val["Pivots"]/4
        val["R1"] = val["Pivots"]/2 - val["low"]
        val["S1"] = val["Pivots"]/2 - val["high"]
      
        df.reset_index(inplace=True)
        
        result = pd.merge(df,val[['date_only']+req],on='date_only',how="left")
        result.drop('date_only',axis='columns', inplace=True)

        
        return result[req]

    def Woodie_CPR(self,df,his,req=["Pivot","R1","S1","R2","S2","R3","S3","R4","S4"]) :
        
        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])

        val = his.copy()
        #val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        # val["open"] = val.open.shift(1)
        val["Pivot"] = (val["high"] + val["low"] + (val["open"]*2))/4
        val["R1"] = 2*val["Pivot"] - val["low"] 
        val["S1"] = 2*val["Pivot"] - val["high"]
        val["R2"] = val["Pivot"] + (val["high"] - val["low"])
        val["S2"] = val["Pivot"] - (val["high"] - val["low"])
        val["R3"] = val["high"] + 2 * (val["Pivot"] - val["low"])
        val["S3"] = val["low"] - 2 * (val["high"] - val["Pivot"])
        val["R4"] = val["high"] + 2 * (val["Pivot"] - val["low"]) + (val["high"] - val["low"])
        val["S4"] = val["low"] - 2 * (val["high"] - val["Pivot"]) + (val["high"] - val["low"])      
        df.reset_index(inplace=True)
        
        result = pd.merge(df,val[['date_only']+req],on='date_only',how="left")
        result.drop('date_only',axis='columns', inplace=True)

        
        return result[req]
    def Fibonacci_CPR(self,df,his,req=["Pivot","R1","S1","R2","S2","R3","S3"]) :
        # print("In fibonacci")
        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        val = his.copy()
        val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
       
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        val["close"] = val.close.shift(1)
        val["Pivot"] = (val["high"] + val["low"] + val["close"])/3
        val["H_L"] = val["high"] - val["low"]
        val["R1"] = val["Pivot"] + 0.382*val["H_L"]
        val["S1"] = val["Pivot"] - 0.382*val["H_L"]
        val["R2"] = val["Pivot"] + 0.618*val["H_L"]
        val["S2"] = val["Pivot"] - 0.618*val["H_L"]
        val["R3"] = val["Pivot"] + val["H_L"]
        val["S3"] = val["Pivot"] - val["H_L"]
        
        df.reset_index(inplace=True)

        result = pd.merge(df,val[['date_only']+req],on='date_only',how='left')
        result.drop('date_only',axis='columns', inplace=True)        
        return result[req]
    def Camarilla_CPR(self,df,his,req=["R2","S2","R3","S3","R4","S4"]) :
        
        df['date'] = pd.to_datetime(df['date'])
        df["date_only"] = df['date'].dt.date
        df = df.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])
        his['date'] = pd.to_datetime(his['date'])
        his["date_only"] = his['date'].dt.date
        his = his.set_index('date')
        dict1 = OrderedDict([('date_only', 'first'),('open', 'first'),('high', 'max'),('low', 'min'),('close', 'last'),('volume', 'sum')])

        val = his.copy()
        #val = val.resample('1D').agg(dict1)
        val.dropna(inplace=True)
        val.reset_index( inplace=True)
        val["high"] = val.high.shift(1)
        val["low"] = val.low.shift(1)
        val["close"] = val.close.shift(1)
        val["Pivot"] = (val["high"] + val["low"] + val["close"])/3
        val["R1"] = val["close"] + ((val["high"] - val["low"])*1.1/12.0)
        val["S1"] = val["close"] - ((val["high"] - val["low"])*1.1/12.0)
        val["R2"] = val["close"] + ((val["high"] - val["low"])*1.1/6.0)
        val["S2"] = val["close"] - ((val["high"] - val["low"])*1.1/6.0)
        val["R3"] =val["close"] + ((val["high"] - val["low"])*1.1/4.0)
        val["S3"] = val["close"] - ((val["high"] - val["low"])*1.1/4.0)
        val["R4"] = val["close"] + ((val["high"] - val["low"])*1.1/2.0)
        val["S4"] = val["close"] - ((val["high"] - val["low"])*1.1/2.0)
      
        df.reset_index(inplace=True)
        
        result = pd.merge(df,val[['date_only']+req],on='date_only',how="left")
        result.drop('date_only',axis='columns', inplace=True)

        
        return result[req]
    def ALLIGATOR(self,data) :
        
        # print("Received data")
        jaw = self.SMMA((data["high"]+data["low"])/2,13)
        jaw = self.POSITION(jaw,8)
        teeth = self.SMMA((data["high"]+data["low"])/2,8)
        teeth = self.POSITION(teeth,5)
        lips = self.SMMA((data["high"]+data["low"])/2,5)
        lips = self.POSITION(lips,3)
        

        return jaw , teeth , lips
    
    def DONCHAIN_CHANNEL(self,data,periods) :
        
        UC = self.MAX(data["high"],periods)
        LC = self.MIN(data["low"],periods)
        MID = (UC + LC)/2
        
        return UC , MID , LC
    
    def MAX(self,data,periods) :
        return data.rolling(periods).max()
    
    def MIN(self,data,periods) :
        return data.rolling(periods).min()
    
    def POSITION(self,data,periods) :
        return data.shift(periods)


    def TRENDRTUBER_BANDS(self,data,lookback) :
        
        high_list = []
        low_list = []
        
        data["high_1"] = self.POSITION(data.high,1)
        data["high_2"] = self.POSITION(data.high,2)
        data["high_3"] = self.POSITION(data.high,3)
        data["high_4"] = self.POSITION(data.high,4)
        
        data["low_1"] = self.POSITION(data.low,1)
        data["low_2"] = self.POSITION(data.low,2)
        data["low_3"] = self.POSITION(data.low,3)
        data["low_4"] = self.POSITION(data.low,4)
        
        data["th"] = 0.0
        data["bl"] = 0.0
        data["lowline"] = 0.0
        data["highline"] = 0.0
        
        
        mask = (data["high_2"]>data["high_1"]) & (data["high_2"]>data["high"]) & (data["high_2"]>data["high_3"]) & (data["high_2"]>data["high_4"])
        data.loc[mask,"th"] = -1
        
        mask = (data["low_2"]<data["low_1"]) & (data["low_2"]<data["low"]) & (data["low_2"]<data["low_3"]) & (data["low_2"]<data["low_4"])
        data.loc[mask,"bl"] = 1
        
        data["tot"] = data["th"] + data["bl"]
        
        
        for index,row in data.iterrows():
            if row["tot"] == 1 :
                low_list.append(row["low_2"])
            elif row["tot"] == -1 :
                high_list.append(row["high_2"])
            data.at[index,"lowline"] = sum(low_list[-1*lookback:])/lookback
            data.at[index,"highline"] = sum(high_list[-1*lookback:])/lookback
            
        return data["lowline"] , data["highline"]
