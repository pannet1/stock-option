from omspy_brokers.bypass import Bypass
from aliceblue3 import *
from logzero import logger
import pandas as pd
from toolkit.fileutils import Fileutils
import csv
from datetime import timedelta, datetime, date
import time
from indicators import indicators
import samco_mapper
import sys

pd.set_option('display.max_rows', None)
FUTL = Fileutils()
DATA_FOLDER = "../data/"
SETG = FUTL.get_lst_fm_yml(DATA_FOLDER+"ravikanth_creds.yml")

print(SETG)

def calc_ATM(STOCK,ltp) :
    ATM = round(ltp/diff[STOCK])*diff[STOCK]
    return ATM

def Resample_data(stk_name,periods,offset,hist_data_arg=None) :
    global candles_1 , STOCK_TO_ZERODHA_SPOT
    df = pd.DataFrame.from_dict(candles_1[stk_name], orient ='index')
    df.reset_index(inplace = True)
    df = df.rename(columns={'index': 'dateTime'})
    df["dateTime"] = pd.to_datetime(df['dateTime'],format="%d-%m-%Y %H:%M:%S")
    # df = samco_data_source.resample_data(df,periods,offset)
    if hist_data_arg == None :
        frames = [hist_data[stk_name],df]    
        
    result = pd.concat(frames)
    result.reset_index(drop = True , inplace = True)
    result = samco_data_source.resample_data(result,periods,offset)
    # result = result.rename(columns={'dateTime': 'date'})
    return result.copy()


def round_it(x):
    return (round(x, 2)*100//5*5)/100

def HeikinAshi(data):
    result = data[['dateTime','open','high','low','close','volume']].copy()
    result['close'] = round_it(((data['open'] + data['high'] + data['low'] + data['close'])/4))
    
    for j in range(len(data)):
        if j == 0:
            result.iat[0,1] = round_it(((data['open'].iloc[0] + data['close'].iloc[0])/2))
        else:
            result.iat[j,1] = round_it(((result.iat[j-1,1] + result.iat[j-1,4])/2))

    result['high'] = result.loc[:,['open', 'close']].join(data['high']).max(axis=1)
    result['low'] = result.loc[:,['open', 'close']].join(data['low']).min(axis=1)
    
    col_list = result.columns.values.tolist()
    col_mapper = {j: i for i, j in enumerate(col_list)}
    return result , col_mapper 
    

ZERODHA_CREDS = SETG["zerodha"]
ALICEBLUE_CREDS = SETG["aliceblue"]
STRATEGY_DATA = []
with open(DATA_FOLDER + 'STRATEGY_DATA.csv') as csv_file :
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        STRATEGY_DATA.append(int(float(row[1])))   

BROKER = Bypass
kite_ = BROKER(**ZERODHA_CREDS)
if not kite_.authenticate():
    logger.error("kite authentication Failed!")
    sys.exit()
# kite = KiteConnect(api_key=api_key)
# request_token = login.kiteLogin(user_name, password, totp, api_key, api_secret)
# data = kite.generate_session(request_token, api_secret=api_secret)
# kite.set_access_token(data["access_token"])
# kws = KiteTicker(api_key, data["access_token"])  
kite = kite_.kite     
kws = kite.kws()

alice = Aliceblue(user_id=ALICEBLUE_CREDS['username'], api_key=ALICEBLUE_CREDS["api_secret"])
logger.debug(alice.get_session_id())
logger.debug("Aliceblue login successful")
print("Aliceblue login")

alice.get_contract_master(exchange="NFO")

LIVE = STRATEGY_DATA[0]
BOLLINGER_BAND_PERIODS = STRATEGY_DATA[1]
BOLLINGER_BAND_MULIPLIER = STRATEGY_DATA[2]
RAW_STOCK_LIST = []
INPUT_SPOT_LIST = []
INPUT_SPOT_TO_SPOT = {"NIFTY":"NIFTY 50","BANKNIFTY":"NIFTY BANK"}
diff = {"BANKNIFTY":100 , "NIFTY":50}
DEVIATION = {"NIFTY":STRATEGY_DATA[3],"BANKNIFTY":STRATEGY_DATA[3]}
LTP = {}
TRADE_TAKEN = {}
SL_MARK = {}
LIMIT_STORE = {}
ENTRY_CANDLE = {}
InstrumentID = {}
ENTRY_INSTRUMENT = {}
candles_1 = {}
hist_data = {}
tokens = []

global_start_time = datetime.now().replace(hour=9, minute=15,second=0, microsecond=0)
exit_time = datetime.now().replace(hour=15, minute=25 , second=0 , microsecond=0)
max_expiry_entry_time = datetime.now().replace(hour=14, minute=0 , second=0 , microsecond=0)
exit_triggered = False


today = date.today()           #-timedelta(WHETHER_CHECKING_NEXT_DAY)
start_date=(today-timedelta(days=15)).strftime("%Y-%m-%d  09:15:00")
end_date=(today).strftime("%Y-%m-%d  15:29:00")



with open(DATA_FOLDER + 'STOCK_LIST_DATA.csv') as csv_file :
    csv_reader = csv.reader(csv_file)
    for row in csv_reader:
        if len(row) > 0 :
            RAW_STOCK_LIST.append(row)


INPUT_SPOT_LIST = [x[0] for x in RAW_STOCK_LIST]
LOTS = {x[0]:int(float(x[1])) for x in RAW_STOCK_LIST}

data_received = False
while not data_received :
    try :
        InstrumentInfo=kite.instruments(exchange='NFO')
        data_received = True
    except Exception as e :
        pass



samco_data_source = samco_mapper.samco_data()
technicalIndicators = indicators()

data1 = pd.DataFrame(InstrumentInfo)

data1['expiry'] = data1['expiry'].apply(lambda x: x.strftime("%Y-%m-%d"))

data_expiry = data1[(data1['tradingsymbol'].str.contains('RELIANCE') == True) & (data1["segment"]=="NFO-FUT")]


expiry_list = list(set(data_expiry.expiry.values.tolist()))
expiry_list.sort()
currentMonthExpiry = expiry_list[0]
currentMonthExpiry = datetime.strptime(currentMonthExpiry, "%Y-%m-%d").date()
print(currentMonthExpiry)


ZERODHA_INSTRUMENT_DATA = pd.DataFrame(InstrumentInfo)
ZERODHA_INSTRUMENT_DATA = ZERODHA_INSTRUMENT_DATA[((ZERODHA_INSTRUMENT_DATA['segment'] == "NFO-FUT"))]
# print(ZERODHA_INSTRUMENT_DATA['expiry'].values.tolist())
ZERODHA_INSTRUMENT_DATA = ZERODHA_INSTRUMENT_DATA[((ZERODHA_INSTRUMENT_DATA['expiry'] == currentMonthExpiry))]
print("hi")
# ZERODHA_INSTRUMENT_DATA.to_csv("111.csv")


for instrument in INPUT_SPOT_LIST :
    
        
    try :
        ins_token = int(ZERODHA_INSTRUMENT_DATA.loc[(ZERODHA_INSTRUMENT_DATA['name']==instrument)]['instrument_token'].iloc[0])
        trading_symbol = ZERODHA_INSTRUMENT_DATA.loc[(ZERODHA_INSTRUMENT_DATA['name']==instrument)]['tradingsymbol'].iloc[0]
    except Exception as e:  
        print(e)
        print("Skipping instrument {} due to its absence in Futures".format(instrument))
        logger.debug("Skipping instrument {} due to its absence in Futures".format(instrument))
        continue

    TRADE_TAKEN[instrument] = "None"
    candles_1[trading_symbol] = {}
    InstrumentID[trading_symbol] = ins_token
    tokens.append(ins_token)
    
    _df = kite.historical_data(InstrumentID[trading_symbol],start_date,end_date,"minute")
    # pd.DataFrame(_df).to_csv("historical_data_check.csv")
    hist_data[trading_symbol] = _df
    hist_data[trading_symbol] = pd.DataFrame(hist_data[trading_symbol])
    hist_data[trading_symbol]["date"] = hist_data[trading_symbol]["date"].dt.strftime("%d-%m-%Y %H:%M:%S")
    hist_data[trading_symbol]["date"] = pd.to_datetime(hist_data[trading_symbol]["date"],format="%d-%m-%Y %H:%M:%S")
    hist_data[trading_symbol] = hist_data[trading_symbol].rename(columns={'date': 'dateTime'})



def on_ticks(ws,ticks) :
    
    print(ticks)
    for tick in ticks:
        instrument_type=tick["instrument_token"]
        ltt=tick["exchange_timestamp"]
        min1 = 1
        ltt_min_1=datetime(ltt.year, ltt.month, ltt.day, ltt.hour,ltt.minute//min1*min1)
        ltp = round_it(tick["last_price"])
        instrument = list(InstrumentID.keys())[list(InstrumentID.values()).index(instrument_type)]
        LTP[instrument] = ltp
        volume = 0

        if ltt >= global_start_time :
            try:
                if ltt_min_1 in candles_1[instrument]:
                    candles_1[instrument][ltt_min_1]["high"]=max(candles_1[instrument][ltt_min_1]["high"],ltp) #1
                    candles_1[instrument][ltt_min_1]["volume"]=max(candles_1[instrument][ltt_min_1]["volume"],tick["volume"]) #1.5 Use the max in volume instead of last.
                    candles_1[instrument][ltt_min_1]["low"]=min(candles_1[instrument][ltt_min_1]["low"],ltp) #2
                    candles_1[instrument][ltt_min_1]["close"]=ltp #3
                    candles_1[instrument][ltt_min_1]["volume"]=volume #3.5
                else:
                    candles_1[instrument][ltt_min_1]={}
                    candles_1[instrument][ltt_min_1]["high"]=ltp #4
                    candles_1[instrument][ltt_min_1]["low"]=ltp #5
                    candles_1[instrument][ltt_min_1]["open"]=ltp #6
                    candles_1[instrument][ltt_min_1]["close"]=ltp #7
                    candles_1[instrument][ltt_min_1]["volume"]=volume #3.5
            except KeyError:

                if instrument not in candles_1:
                    candles_1[instrument]={}
                if ltt_min_1 not in candles_1[instrument]:
                    candles_1[instrument][ltt_min_1]={}
                    candles_1[instrument][ltt_min_1]["high"]=ltp #8
                    candles_1[instrument][ltt_min_1]["low"]=ltp #9
                    candles_1[instrument][ltt_min_1]["open"]=ltp #10
                    candles_1[instrument][ltt_min_1]["close"]=ltp #11
                    candles_1[instrument][ltt_min_1]["volume"]=volume #3.5

def on_connect(ws, response):
    # Callback on successful connect.
    # Subscribe to a list of instrument_tokens (RELIANCE and ACC here).
    print("On connect tokens :",tokens)
    ws.subscribe(tokens)
    # Set RELIANCE to tick in `full` mode.
    ws.set_mode(ws.MODE_FULL, tokens)

def on_close(ws, code, reason):
    # On connection close stop the main loop
    # Reconnection will not happen after executing `ws.stop()`
    global exit_triggered
    if exit_triggered :
        print("Exit time  : ",datetime.now())
        ws.stop()



def PlaceEntryOrder(instrument,entry_type,price):
    STRIKE = calc_ATM(instrument,price) + (1 if entry_type == 'BUY' else -1) * diff[instrument] * DEVIATION[instrument]
    is_CE = entry_type == 'BUY'
    ENTRY_INSTRUMENT[instrument] =  alice.get_instrument_for_fno(symbol = instrument, expiry_date=currentMonthExpiry.strftime("%d-%m-%Y"), is_fut=False, strike=STRIKE, is_CE = is_CE, exch="NFO")
    qty = int(ENTRY_INSTRUMENT[instrument].lot_size)*LOTS[instrument]
    print(ENTRY_INSTRUMENT[instrument],"  qty=  ",qty)
    logger.debug("ENTRY order in {} in {} DIRECTION , strike = {} , qty = {}".format(instrument,entry_type,STRIKE,qty))

    if LIVE == 1 :
        order_data = alice.place_order(transaction_type = TransactionType.Buy,
                                instrument = ENTRY_INSTRUMENT[instrument],
                                quantity = qty,
                                order_type = OrderType.Market,
                                product_type = ProductType.Delivery,
                                price = 0.0,
                                trigger_price = None,
                                stop_loss = None,
                                square_off = None,
                                trailing_sl = None,
                                is_amo = False)
        logger.debug("Order details {}".format(order_data))
        


def ExitStock(instrument):
    qty = int(ENTRY_INSTRUMENT[instrument].lot_size)*LOTS[instrument]
    if LIVE == 1 :
        order_data = alice.place_order(transaction_type = TransactionType.Sell,
                                instrument = ENTRY_INSTRUMENT[instrument],
                                quantity = qty,
                                order_type = OrderType.Market,
                                product_type = ProductType.Delivery,
                                price = 0.0,
                                trigger_price = None,
                                stop_loss = None,
                                square_off = None,
                                trailing_sl = None,
                                is_amo = False)
        logger.debug("Order details {}".format(order_data))

    pass

kws.on_ticks = on_ticks
kws.on_connect = on_connect
kws.on_close = on_close

while datetime.now() < global_start_time :
    pass

kws.connect(threaded=True)

time.sleep(5)
while datetime.now() < exit_time :
    
    for instrument in INPUT_SPOT_LIST :
        if TRADE_TAKEN[instrument] !="None" :

            try :
                fiveMin_HA ,fiveMin_col_mapper = HeikinAshi(Resample_data(INPUT_SPOT_TO_SPOT[instrument],"1T",'0T'))
            except :
                continue
            UBB,MBB,LBB = technicalIndicators.BOLLINGER_BAND(fiveMin_HA.close,BOLLINGER_BAND_PERIODS,BOLLINGER_BAND_MULIPLIER)
            fiveMin_HA_HA_data_len = len(fiveMin_HA)
            if ENTRY_CANDLE[instrument] == fiveMin_HA.iat[-1,fiveMin_col_mapper['dateTime']]:
                continue

            if TRADE_TAKEN[instrument] == "BUY" :
                Exit11 = fiveMin_HA.iat[-2,fiveMin_col_mapper['high']] > MBB.iat[-2] #Daily HA prev candle high above MBB 
                Exit12 = fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] < MBB.iat[-2] #Daily HA prev candle close below MBB 
                Exit13 = fiveMin_HA.iat[-2,fiveMin_col_mapper['open']] > fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] #HA candle is red in colour 
                Exit2 = fiveMin_HA.iat[-1,fiveMin_col_mapper['close']] < SL_MARK[instrument] # HA Price Close below Low of Entry HA Red Candle , SL HIT
                
                # if Exit11 and Exit12 and Exit13 :
                #     ExitStock(instrument)
                #     TRADE_TAKEN[instrument] = "None"
                #     logger.debug("Exit of {} due to MBB".format(instrument))
                #     continue
                if Exit2 :
                    ExitStock(instrument)
                    TRADE_TAKEN[instrument] = "None"
                    logger.debug("Exit of {} due to SL HIT".format(instrument))
                    continue
                else :
                    if LIMIT_STORE[instrument] == -1 :
                        check1 = fiveMin_HA.iat[-2,fiveMin_col_mapper['high']] > UBB.iat[-2] #Daily HA prev candle high above UBB 
                        # check2 = fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] < UBB.iat[-2] #Daily HA prev candle close below UBB 
                        if check1:
                            LIMIT_STORE[instrument] = fiveMin_HA.iat[-2,fiveMin_col_mapper['low']]
                            logger.debug(("Storing mark value for crossing UBB of {} as {}".format(instrument,LIMIT_STORE[instrument])))
                    elif fiveMin_HA.iat[-2,fiveMin_col_mapper['low']] > LIMIT_STORE[instrument] :
                        LIMIT_STORE[instrument] = fiveMin_HA.iat[-2,fiveMin_col_mapper['low']]
                        logger.debug(("Updating mark value for having a higher low of {} as {}".format(instrument,LIMIT_STORE[instrument])))
                    elif fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] < LIMIT_STORE[instrument] :
                        ExitStock(instrument)
                        TRADE_TAKEN[instrument] = "None"
                        logger.debug("Exit of {} due to UBB".format(instrument))
                        continue
                
            elif TRADE_TAKEN[instrument] == "SELL" :
                Exit11 = fiveMin_HA.iat[-2,fiveMin_col_mapper['low']] < MBB.iat[-2] #Daily HA prev candle low below MBB 
                Exit12 = fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] > MBB.iat[-2] #Daily HA prev candle close above MBB 
                Exit13 = fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] > fiveMin_HA.iat[-2,fiveMin_col_mapper['open']] #HA candle is green in colour 
                Exit2 = fiveMin_HA.iat[-1,fiveMin_col_mapper['close']] > SL_MARK[instrument] # HA Price Close above High of Entry HA Green Candle , SL HIT
                
                # if Exit11 and Exit12 and Exit13 :
                #     ExitStock(instrument)
                #     TRADE_TAKEN[instrument] = "None"

                #     logger.debug("Exit of {} due to MBB".format(instrument))
                #     continue
                if Exit2 :
                    ExitStock(instrument)
                    TRADE_TAKEN[instrument] = "None"

                    logger.debug("Exit of {} due to SL HIT".format(instrument))
                    continue
                else :
                    if LIMIT_STORE[instrument] == -1 :
                        check1 = fiveMin_HA.iat[-2,fiveMin_col_mapper['low']] < LBB.iat[-2] #Daily HA prev candle low below LBB 
                        # check2 = fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] > LBB.iat[-2] #Daily HA prev candle close above LBB 
                        if check1:
                            LIMIT_STORE[instrument] = fiveMin_HA.iat[-2,fiveMin_col_mapper['high']]
                            logger.debug(("Storing mark value for crossing LBB of {} as {}".format(instrument,LIMIT_STORE[instrument])))
                    elif fiveMin_HA.iat[-2,fiveMin_col_mapper['high']] < LIMIT_STORE[instrument] :
                        LIMIT_STORE[instrument] = fiveMin_HA.iat[-2,fiveMin_col_mapper['high']]
                        logger.debug(("Updating mark value for having a lower high of {} as {}".format(instrument,LIMIT_STORE[instrument])))
                    elif fiveMin_HA.iat[-2,fiveMin_col_mapper['close']] > LIMIT_STORE[instrument] :
                        ExitStock(instrument)
                        TRADE_TAKEN[instrument] = "None"
                        logger.debug("Exit of {} due to LBB".format(instrument))
                        continue
        
        else :
            try :
                fiveMin_HA ,fiveMin_col_mapper = HeikinAshi(Resample_data(INPUT_SPOT_TO_SPOT[instrument],"1T",'0T'))
                UBB,MBB,LBB = technicalIndicators.BOLLINGER_BAND(fiveMin_HA.close,BOLLINGER_BAND_PERIODS,BOLLINGER_BAND_MULIPLIER)
                fiveMin_HA_HA_data_len = len(fiveMin_HA)
            except :
                continue
            if currentMonthExpiry == datetime.now().date():
                if datetime.now() > max_expiry_entry_time :
                    continue
            
            for i in range(-2,-5,-1) : # Due to 13/08/2022 20:27
                row_num = fiveMin_HA_HA_data_len+i
                BuyCondn1 = LBB.iat[row_num] > fiveMin_HA.iat[row_num,fiveMin_col_mapper['low']]
                # BuyCondn2 = LBB.iat[row_num] < fiveMin_HA.iat[row_num,fiveMin_col_mapper['close']]
                BuyCondn2 = True # Due to 13/08/2022 20:27
                BuyCondn3 = fiveMin_HA.iat[row_num,fiveMin_col_mapper['open']] > fiveMin_HA.iat[row_num,fiveMin_col_mapper['close']]
                BuyCondn4 = fiveMin_HA.iat[-1,fiveMin_col_mapper['close']] > fiveMin_HA.iat[row_num,fiveMin_col_mapper['high']]
                
                SellCondn1 = UBB.iat[row_num] < fiveMin_HA.iat[row_num,fiveMin_col_mapper['high']]
                # SellCondn2 = UBB.iat[row_num] > fiveMin_HA.iat[row_num,fiveMin_col_mapper['close']]
                SellCondn2 = True # Due to 13/08/2022 20:27
                SellCondn3 = fiveMin_HA.iat[row_num,fiveMin_col_mapper['open']] < fiveMin_HA.iat[row_num,fiveMin_col_mapper['close']]
                SellCondn4 = fiveMin_HA.iat[-1,fiveMin_col_mapper['close']] < fiveMin_HA.iat[row_num,fiveMin_col_mapper['low']]

                if BuyCondn1 and BuyCondn2 and BuyCondn3 and BuyCondn4 :
                    print(instrument," buy satisfied on  ",i,"  ",fiveMin_HA.iat[row_num,fiveMin_col_mapper['dateTime']])
                    Strike = calc_ATM(instrument,fiveMin_HA.iat[row_num,fiveMin_col_mapper['close']])
                    SL_MARK[instrument] = min(fiveMin_HA.iat[row_num,fiveMin_col_mapper['low']],fiveMin_HA.iat[-2,fiveMin_col_mapper['low']])
                    TRADE_TAKEN[instrument] = "BUY"
                    LIMIT_STORE[instrument] = -1
                    ENTRY_CANDLE[instrument] = fiveMin_HA.iat[row_num,fiveMin_col_mapper['dateTime']]
                    PlaceEntryOrder(instrument,"BUY",LTP[INPUT_SPOT_TO_SPOT[instrument]])
                    break

                if SellCondn1 and SellCondn2 and SellCondn3 and SellCondn4:
                    print(instrument," sell satisfied on  ",i,"  ",fiveMin_HA.iat[row_num,fiveMin_col_mapper['dateTime']])
                    SL_MARK[instrument] = max(fiveMin_HA.iat[row_num,fiveMin_col_mapper['high']],fiveMin_HA.iat[-2,fiveMin_col_mapper['high']])
                    TRADE_TAKEN[instrument] = "SELL"
                    LIMIT_STORE[instrument] = -1
                    ENTRY_CANDLE[instrument] = fiveMin_HA.iat[row_num,fiveMin_col_mapper['dateTime']]
                    PlaceEntryOrder(instrument,"SELL",LTP[INPUT_SPOT_TO_SPOT[instrument]])
                    break

                    
for instrument in INPUT_SPOT_LIST :
    if TRADE_TAKEN[instrument] !="None" :
        ExitStock(instrument)
        TRADE_TAKEN[instrument] = "None"
        logger.debug("Exit of {} due to Day Exit".format(instrument))
