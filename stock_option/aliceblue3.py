import requests
import json
import hashlib
import enum
import logging
import pandas as pd
from datetime import time,datetime
from time import sleep  
from collections import namedtuple
import websocket
import threading
import math

logger = logging.getLogger(__name__)

Instrument = namedtuple('Instrument', ['exchange', 'token', 'symbol','name', 'expiry', 'lot_size'])


class TransactionType(enum.Enum):
    Buy = 'BUY'
    Sell = 'SELL'

class OrderType(enum.Enum):
    Market = 'MKT'
    Limit = 'L'
    StopLossLimit = 'SL'
    StopLossMarket = 'SL-M'

class ProductType(enum.Enum):
    Intraday = 'MIS'
    Delivery = 'CNC'
    CoverOrder = 'CO'
    BracketOrder = 'BO'
    Normal = 'NRML'


class LiveFeedType(enum.Enum):
    MARKET = 1
    DEPTH = 2

class Resolution(enum.Enum):
    DAY = 1
    MINUTE = 2


def encrypt_string(hashing):
    sha = hashlib.sha256(hashing.encode()).hexdigest()
    return sha



class Aliceblue:
    base_url = "https://ant.aliceblueonline.com/rest/AliceBlueAPIService/api/"
    api_name = "Codifi API Connect - Python Lib "
    version = "1.0.17"
    base_url_c = "https://v2api.aliceblueonline.com/restpy/static/contract_master/%s.csv"

    # Products
    PRODUCT_INTRADAY = "MIS"
    PRODUCT_COVER_ODRER = "CO"
    PRODUCT_CNC = "CNC"
    PRODUCT_BRACKET_ORDER = "BO"
    PRODUCT_NRML = "NRML"

    # Order Type
    REGULAR_ORDER = "REGULAR"
    LIMIT_ORDER = "L"
    STOPLOSS_ORDER = "SL"
    MARKET_ORDER = "MKT"

    # Transaction type
    BUY_ORDER = "BUY"
    SELL_ORDER = "SELL"

    # Positions
    RETENTION_DAY = "DAY" or "NET"

    # Exchanges
    EXCHANGE_NSE = "NSE"
    EXCHANGE_NFO = "NFO"
    EXCHANGE_CDS = "CDS"
    EXCHANGE_BSE = "BSE"
    EXCHANGE_BFO = "BFO"
    EXCHANGE_BCD = "BCD"
    EXCHANGE_MCX = "MCX"

    # Status constants
    STATUS_COMPLETE = "COMPLETE"
    STATUS_REJECTED = "REJECTED"
    STATUS_CANCELLED = "CANCELLED"
    ENC = None
    
    __websocket = None
    subscriptions = None
    # __subscribe_callback = None
    script_subscription_instrument =[]
    ws_connection = False

    
    # response = requests.get(base_url);
    # Getscrip URI

    _sub_urls = {
        # Authorization
        "encryption_key": "customer/getAPIEncpkey",
        "getsessiondata": "customer/getUserSID",

        # Market Watch
        "marketwatch_scrips": "marketWatch/fetchMWScrips",
        "addscrips": "marketWatch/addScripToMW",
        "getmarketwatch_list": "marketWatch/fetchMWList",
        "scripdetails": "ScripDetails/getScripQuoteDetails",
        "getdelete_scrips": "marketWatch/deleteMWScrip",

        # OrderManagement
        "squareoffposition": "positionAndHoldings/sqrOofPosition",
        "position_conversion": "positionAndHoldings/positionConvertion",
        "placeorder": "placeOrder/executePlaceOrder",
        "modifyorder": "placeOrder/modifyOrder",
        "marketorder": "placeOrder/executePlaceOrder",
        "exitboorder": "placeOrder/exitBracketOrder",
        "bracketorder": "placeOrder/executePlaceOrder",
        "positiondata": "positionAndHoldings/positionBook",
        "orderbook": "placeOrder/fetchOrderBook",
        "tradebook": "placeOrder/fetchTradeBook",
        "holding": "positionAndHoldings/holdings",
        "orderhistory": "placeOrder/orderHistory",
        "cancelorder": "placeOrder/cancelOrder",
        "profile": "customer/accountDetails",
        # Funds
        "fundsrecord": "limits/getRmsLimits",
        # Websockey
        "base_url_socket" :"wss://ws1.aliceblueonline.com/NorenWS/",

        #Historical data
        "historicaldata" :"chart/history",
        #LogoutAllDevices
        "logout_all_devices" : "customer/logOutFromAllDevice",
        #Logout API
        "logoutAPI" : "/customer/logout"

    }

    # Common Method
    def __init__(self,
                 user_id,
                 api_key,
                 base=None,
                 session_id=None,
                 disable_ssl=False):

        self.user_id = user_id.upper()
        self.api_key = api_key
        self.disable_ssl = disable_ssl
        self.session_id = session_id
        self.base = base or self.base_url

        self.strike_diff = {}


        self.__keep_running = True
        self.__ws_mutex = threading.Lock()
        self.__subscribers = {}
        self.__websocket_connected = False


        self.__connection_acknowledgement_callback = None
        self.__on_market_subscribe_callback = None
        self.__on_market_feed_tick_callback = None
        self.__on_depth_subscribe_callback = None
        self.__on_depth_feed_tick_callback = None


        self.__on_open = None
        self.__on_disconnect = None
        self.__on_error = None
        self.__subscribe_callback = None
        self.__order_update_callback = None
        self.__market_status_messages_callback = None
        self.__exchange_messages_callback = None
        self.__oi_callback = None
        self.__dpr_callback = None

    def _get(self, sub_url, data=None):
        """Get method declaration"""
        url = self.base + self._sub_urls[sub_url]
        return self._request(url, "GET", data=data)

    def _post(self, sub_url, data=None):
        """Post method declaration"""
        url = self.base + self._sub_urls[sub_url]
        return self._request(url, "POST", data=data)

    def _dummypost(self, url, data=None):
        """Post method declaration"""
        return self._request(url, "POST", data=data)

    def _user_agent(self):
        return self.api_name + self.version

    """Authorization get to call all requests"""
    def _user_authorization(self):
        if self.session_id:
            return "Bearer " + self.user_id.upper() + " " + self.session_id
        else:
            return ""

    """Common request to call POST and GET method"""
    def _request(self, method, req_type, data=None):
        """
        Headers with authorization. For some requests authorization
        is not required. It will be send as empty String
        """
        _headers = {
            "X-SAS-Version": "2.0",
            "User-Agent": self._user_agent(),
            "Authorization": self._user_authorization()
        }
        if req_type == "POST":
            try:
                response = requests.post(method, json=data, headers=_headers, )
            except (requests.ConnectionError, requests.Timeout) as exception:
                return {'stat':'Not_ok','emsg':'Please Check the Internet connection.','encKey':None}
            if response.status_code == 200:
                x = response.text
                return json.loads(response.text)
            else:
                emsg=str(response.status_code)+' - '+response.reason
                return {'stat':'Not_ok','emsg':emsg,'encKey':None}

        elif req_type == "GET":
            try:
                response = requests.get(method, json=data, headers=_headers)
            except (requests.ConnectionError, requests.Timeout) as exception:
                return {'stat':'Not_ok','emsg':'Please Check the Internet connection.','encKey':None}
            txt = response.text
            return json.loads(response.text)

    def _error_response(self,message):
        return {"stat":"Not_ok","emsg":message}
    # Methods to call HTTP Request

    """Userlogin method with userid and userapi_key"""
    def get_session_id(self, data=None):
        data = {'userId': self.user_id.upper()}
        response = self._post("encryption_key", data)
        if response['encKey'] is None:
            return response['emsg']
        else:
            data = encrypt_string(self.user_id.upper() + self.api_key + response['encKey'])
        print(self.user_id.upper(),"  ",response['encKey'])
        data = {'userId': self.user_id.upper(), 'userData': data}
        res = self._post("getsessiondata", data)

        if res['stat'] == 'Ok':
            self.session_id = res['sessionID']
        return res
    
    def logoutAPI(self):
        return self._post("logoutAPI")

    def logoutAllDevices(self):
        return self._post("logout_all_devices")

    """GET Market watchlist"""
    def getmarketwatch_list(self):
        marketwatchrespdata = self._get("getmarketwatch_list")
        return marketwatchrespdata

    """GET Tradebook Records"""
    def get_trade_book(self):
        tradebookresp = self._get("tradebook")
        return tradebookresp

    def get_profile(self):
        profile = self._get("profile")
        return profile

    """GET Holdings Records"""
    def get_holding_positions(self):
        holdingresp = self._get("holding")
        return holdingresp

    """GET Orderbook Records"""
    def order_data(self):
        orderresp = self._get("orderbook")
        return orderresp

    def seacrh_order_by_NstordNo(self,Nstordno,order_list=None):
        if order_list == None:
            order_list = self.order_data()

        for order in order_list:
            if order['Nstordno'] == Nstordno:
                return order['Sym']
        
        return -1
        
    
    def seacrh_order_by_Sym(self,Sym,order_list=None):
        if order_list == None:
            order_list = self.order_data()
        filtered_order_list = []
        for order in order_list:
            if order['Sym'] == Sym:
                filtered_order_list.append(order)
        
        return filtered_order_list
    
    def get_script_history_by_Nstordno(self,Nstordno):
        data = self.order_data()

        stk = self.seacrh_order_by_NstordNo(data,Nstordno)

        if stk == -1:
            return []
        else :
            return self.seacrh_order_by_Sym(data,stk)
        
        
        
        
    def get_order_history(self, nextorder):
        orderresp = self._get("orderbook")
        if nextorder == '':
            # orderresp = self._get("orderbook")
            return orderresp
        else:
            # data = {'nestOrderNumber': nextorder}
            # orderhistoryresp = self._post("orderhistory", data)
            # return orderhistoryresp
            for order in orderresp:
                if order['Nstordno'] == nextorder:
                    return order
    
    def order_history(self,nestOrderNumber):
        data = {'nestOrderNumber':nestOrderNumber}
        orderhistoryresp = self._post("orderhistory",data)
        return orderhistoryresp

    """Method to call Cancel Orders"""
    def cancel_order(self, instrument,nestordernmbr):
        data = {'exch': instrument.exchange,
                'nestOrderNumber': nestordernmbr,
                'trading_symbol': instrument.name}
        cancelresp = self._post("cancelorder", data)
        return cancelresp

    def marketwatch_scripsdata(self, mwname, ):
        data = {'mwName': mwname, }
        marketwatchresp = self._post("marketwatch_scrips", data)
        return marketwatchresp

    """Method to call Add Scrips"""
    def addscrips(self,
                  mwname,
                  exchange,
                  token):
        data = {'mwName': mwname,
                'exch': exchange,
                'symbol': token, }
        addscripsresp = self._post("addscrips", data)
        return addscripsresp

    """Method+ to call Delete Scrips"""
    def deletescrips(self,
                     mwname,
                     exchange,
                     token):
        data = {'mwName': mwname,
                'exch': exchange,
                'symbol': token, }
        deletescripsresp = self._post("getdelete_scrips", data)
        return deletescripsresp

    """Method to call Scrip Details"""
    def get_scrip_info(self,instrument):
        data = {'exch': instrument.exchange,
                'symbol': str(instrument.token)}
        scripsdetailresp = self._post("scripdetails", data)
        return scripsdetailresp

    """Method to call Squareoff Positions"""
    def squareoff_positions(self,
                            exchange,
                            pCode,
                            qty,
                            tokenno,
                            symbol):
        data = {'exchSeg': exchange,
                'pCode': pCode,
                'netQty': qty,
                'tockenNo': tokenno,
                'symbol': symbol}
        squareoffresp = self._post("squareoffposition", data)
        return squareoffresp

    """Method to call  Place Order"""
    def place_order(self, transaction_type, instrument, quantity, order_type,
                    product_type, price=0.0, trigger_price=None,
                    stop_loss=None, square_off=None, trailing_sl=None,
                    is_amo=False,
                    order_tag=None,
                    is_ioc=False):
        if transaction_type is None:
            raise TypeError("Required parameter transaction_type not of type TransactionType")

        if instrument is None:
            raise TypeError("Required parameter instrument not of type Instrument")

        if not isinstance(quantity, int):
            raise TypeError("Required parameter quantity not of type int")

        if order_type is None:
            raise TypeError("Required parameter order_type not of type OrderType")

        if product_type is None:
            raise TypeError("Required parameter product_type not of type ProductType")

        if price is not None and not isinstance(price, float):
            raise TypeError("Optional parameter price not of type float")

        if trigger_price is not None and not isinstance(trigger_price, float):
            raise TypeError("Optional parameter trigger_price not of type float")
        if is_amo == True:
            complexty = "AMO"
        else:
            complexty = "regular"
        discqty=0
        exch=instrument.exchange
        if (instrument.exchange == 'NFO' or instrument.exchange == 'MCX')and (product_type.value == 'CNC'):
            pCode = "NRML"
        else:
            pCode = product_type.value
        price = price
        prctyp = order_type.value
        qty = quantity
        if is_ioc:
            ret='IOC'
        else:
            ret='DAY'
        trading_symbol=instrument.name
        symbol_id=str(instrument.token)
        transtype=transaction_type.value
        trigPrice=trigger_price
        # print("pCode:",instrument)
        data = [{'complexty': complexty,
                 'discqty': discqty,
                 'exch': exch,
                 'pCode': pCode,
                 'price': price,
                 'prctyp': prctyp,
                 'qty': qty,
                 'ret': ret,
                 'symbol_id': symbol_id,
                 'trading_symbol': trading_symbol,
                 'transtype': transtype,
                 "stopLoss": stop_loss,
                 "target": square_off,
                 "trailing_stop_loss": trailing_sl,
                 "trigPrice": trigPrice,
                 "orderTag":order_tag}]
        # print(data)
        placeorderresp = self._post("placeorder", data)
        return placeorderresp


    """Method to get Funds Data"""

    def get_balance(self):
        fundsresp = self._get("fundsrecord")
        return fundsresp

    """Method to call Modify Order"""

    def modify_order(self, transaction_type, instrument, product_type, order_id, order_type, quantity, price=0.0,trigger_price=0.0):
        if not isinstance(instrument, Instrument):
            raise TypeError("Required parameter instrument not of type Instrument")

        if not isinstance(order_id, str):
            raise TypeError("Required parameter order_id not of type str")

        if not isinstance(quantity, int):
            raise TypeError("Optional parameter quantity not of type int")

        if type(order_type) is not OrderType:
            raise TypeError("Optional parameter order_type not of type OrderType")

        if ProductType is None:
            raise TypeError("Required parameter product_type not of type ProductType")

        if price is not None and not isinstance(price, float):
            raise TypeError("Optional parameter price not of type float")

        if trigger_price is not None and not isinstance(trigger_price, float):
            raise TypeError("Optional parameter trigger_price not of type float")
        data = {'discqty': 0,
                'exch': instrument.exchange,
                # 'filledQuantity': filledQuantity,
                'nestOrderNumber': order_id,
                'prctyp': order_type.value,
                'price': price,
                'qty': quantity,
                'trading_symbol': instrument.name,
                'trigPrice': trigger_price,
                'transtype': transaction_type.value,
                'pCode': product_type.value}
        # print(data)
        modifyorderresp = self._post("modifyorder", data)
        return modifyorderresp

    """Method to call Exitbook  Order"""

    def exitboorder(self,nestOrderNumber,symbolOrderId,status, ):
        data = {'nestOrderNumber': nestOrderNumber,
                'symbolOrderId': symbolOrderId,
                'status': status, }
        exitboorderresp = self._post("exitboorder", data)
        return exitboorderresp

    """Method to get Position Book"""

    def positionbook(self,ret, ):
        data = {'ret': ret, }
        positionbookresp = self._post("positiondata", data)
        return positionbookresp

    def get_daywise_positions(self):
        data = {'ret': 'DAY' }
        positionbookresp = self._post("positiondata", data)
        return positionbookresp

    def get_netwise_positions(self,):
        data = {'ret': 'NET' }
        positionbookresp = self._post("positiondata", data)
        return positionbookresp

    def place_basket_order(self,orders):
        data=[]
        for i in range(len(orders)):
            order_data = orders[i]
            if 'is_amo' in order_data and order_data['is_amo']:
                complexty = "AMO"
            else:
                complexty = "regular"
            discqty = 0
            exch = order_data['instrument'].exchange
            if order_data['instrument'].exchange == 'NFO' and order_data['product_type'].value == 'CNC':
                pCode = "NRML"
            else:
                pCode = order_data['product_type'].value
            price = order_data['price'] if 'price' in order_data else 0

            prctyp = order_data['order_type'].value
            qty = order_data['quantity']
            if 'is_ioc' in order_data and order_data['is_ioc']:
                ret = 'IOC'
            else:
                ret = 'DAY'
            trading_symbol = order_data['instrument'].name
            symbol_id = str(order_data['instrument'].token)
            transtype = order_data['transaction_type'].value
            trigPrice = order_data['trigger_price'] if 'trigger_price' in order_data else None
            stop_loss = order_data['stop_loss'] if 'stop_loss' in order_data else None
            trailing_sl = order_data['trailing_sl'] if 'trailing_sl' in order_data else None
            square_off = order_data['square_off'] if 'square_off' in order_data else None
            ordertag = order_data['order_tag'] if 'order_tag' in order_data else None
            request_data={'complexty': complexty,
                     'discqty': discqty,
                     'exch': exch,
                     'pCode': pCode,
                     'price': price,
                     'prctyp': prctyp,
                     'qty': qty,
                     'ret': ret,
                     'symbol_id': symbol_id,
                     'trading_symbol': trading_symbol,
                     'transtype': transtype,
                     "stopLoss": stop_loss,
                     "target": square_off,
                     "trailing_stop_loss": trailing_sl,
                     "trigPrice": trigPrice,
                     "orderTag":ordertag}

            data.append(request_data)
        # print(data)
        placeorderresp = self._post("placeorder", data)
        return placeorderresp

    def get_contract_master(self,exchange):
        if len(exchange) == 3 or exchange == 'INDICES':
            print("NOTE: Today's contract master file will be updated after 08:00 AM. Before 08:00 AM previous day contract file be downloaded.")
            if time(8,00) <= datetime.now().time():
                url= self.base_url_c % exchange.upper()
                response = requests.get(url)
                with open("%s.csv"% exchange.upper(), "w") as f:
                    f.write(response.text)
                return self._error_response("Today contract File Downloaded")
            else:
                return self._error_response("Previous day contract file saved")
        elif exchange is None:
            return self._error_response("Invalid Exchange parameter")
        else:
            return self._error_response("Invalid Exchange parameter")

    def get_instrument_by_symbol(self,exchange, symbol):
        try:
            contract = pd.read_csv("%s.csv" % exchange)
        except OSError as e:
            if e.errno == 2:
                self.get_contract_master(exchange)
                contract = pd.read_csv("%s.csv" % exchange)
            else:
                return self._error_response(e)
        if exchange == 'INDICES':
            filter_contract = contract[contract['symbol'] == symbol.upper()]
            if len(filter_contract) == 0:
                return self._error_response("The symbol is not available in this exchange")
            else:
                filter_contract = filter_contract.reset_index()
                inst = Instrument(filter_contract['exch'][0], filter_contract['token'][0], filter_contract['symbol'][0],
                                  '', '', '')
                return inst
        else:
            filter_contract = contract[contract['Symbol'] == symbol.upper()]
            if len(filter_contract) == 0:
                return self._error_response("The symbol is not available in this exchange")
            else:
                filter_contract = filter_contract.reset_index()
                if 'expiry_date' in filter_contract:
                    inst = Instrument(filter_contract['Exch'][0], filter_contract['Token'][0],
                                      filter_contract['Symbol'][0], filter_contract['Trading Symbol'][0],
                                      filter_contract['Expiry Date'][0], filter_contract['Lot Size'][0])
                else:
                    inst = Instrument(filter_contract['Exch'][0], filter_contract['Token'][0],
                                      filter_contract['Symbol'][0], filter_contract['Trading Symbol'][0], '',
                                      filter_contract['Lot Size'][0])
                return inst

    def get_instrument_by_token(self,exchange, token):
        try:
            contract = pd.read_csv("%s.csv" % exchange)
        except OSError as e:
            if e.errno == 2:
                self.get_contract_master(exchange)
                contract = pd.read_csv("%s.csv" % exchange)
            else:
                return self._error_response(e)
        if exchange == 'INDICES':
            filter_contract = contract[contract['token'] == token]
            inst = Instrument(filter_contract['exch'][0], filter_contract['token'][0], filter_contract['symbol'][0],'', '','')
            return inst
        else:
            filter_contract = contract[contract['Token'] == token]
            if len(filter_contract) == 0:
                return self._error_response("The symbol is not available in this exchange")
            else:
                filter_contract = filter_contract.reset_index()
                if 'expiry_date' in filter_contract:
                    inst = Instrument(filter_contract['Exch'][0], filter_contract['Token'][0], filter_contract['Symbol'][0],
                                      filter_contract['Trading Symbol'][0], filter_contract['Expiry Date'][0],
                                      filter_contract['Lot Size'][0])
                else:
                    inst = Instrument(filter_contract['Exch'][0], filter_contract['Token'][0], filter_contract['Symbol'][0],
                                      filter_contract['Trading Symbol'][0], '', filter_contract['Lot Size'][0])
                return inst

    def get_instrument_for_fno(self,exch,symbol, expiry_date,is_fut=True,strike=None,is_CE = False):
        # print(exch)
        if exch in ['NFO','CDS','MCX','BFO','BCD']:
            if exch == 'CDS':
                edate_format='%d-%m-%Y'
            else:
                edate_format = '%Y-%m-%d'
        else:
            return self._error_response("Invalid exchange")
        if not symbol:
            return self._error_response("Symbol is Null")
        try:
            expiry_date=datetime.strptime(expiry_date, "%d-%m-%Y").date()
        except ValueError as e:
            return self._error_response(e)
        if type(is_CE) is bool:
            if is_CE == True:
                option_type="CE"
            else:
                option_type="PE"
        else:
            return self._error_response("is_CE is not boolean value")
        # print(option_type)
        try:
            contract = pd.read_csv("%s.csv" % exch)
            # print(strike,is_fut)
        except OSError as e:
            if e.errno == 2:
                self.get_contract_master(exch)
                contract = pd.read_csv("%s.csv" % exch)
            else:
                return self._error_response(e)
        if is_fut == False:
            if strike:
                filter_contract = contract[(contract['Exch'] == exch)&(contract['Symbol'] == symbol)&(contract['Option Type'] == option_type)&(contract['Strike Price'] == strike)&(contract['Expiry Date'] == expiry_date.strftime(edate_format))]
            else:
                filter_contract = contract[(contract['Exch'] == exch)&(contract['Symbol'] == symbol)&(contract['Option Type'] == option_type)&(contract['Expiry Date'] == expiry_date.strftime(edate_format))]
        if is_fut == True:
            if strike == None:
                filter_contract = contract[(contract['Exch'] == exch)&(contract['Symbol'] == symbol)&((
                    contract['Strike Price'] == 0) | (contract['Strike Price'] == -1))&(contract['Expiry Date'] == expiry_date.strftime(edate_format))]
            else:
                return self._error_response("No strike price for future")
        # print(len(filter_contract))
        if len(filter_contract) == 0:
            return self._error_response("No Data")
        else:
            inst=[]
            filter_contract = filter_contract.reset_index()
            for i in range(len(filter_contract)):
                inst.append(Instrument(filter_contract['Exch'][i], filter_contract['Token'][i], filter_contract['Symbol'][i], filter_contract['Trading Symbol'][i], filter_contract['Expiry Date'][i],filter_contract['Lot Size'][i]))
            if len(inst) == 1:
                return inst[0]
            else:
                return inst

    def get_stored_master_data(self,exchange):
        try:
            return pd.read_csv("{}.csv".format(exchange))
        except OSError as e:
            if e.errno == 2:
                self.get_contract_master("{}".format(exchange))
                return pd.read_csv("{}.csv".format(exchange))
            else:
                return self._error_response(e)

    def find_ATM(self,symbol,price) :

        if symbol not in self.strike_diff :
            self.strike_diff.update({symbol:self.get_strike_diff(symbol)})
        strike_diff = self.strike_diff[symbol]
        return round(price/strike_diff) * strike_diff

    def find_strike(self, symbol,price,deviation=0):
        ATM = self.find_ATM(symbol,price)
        strike_diff = self.strike_diff[symbol]
        return ATM + deviation*strike_diff
    def get_opt_inst_n_deviation(self,symbol,price,deviation,expiry_date=None) :
        if deviation <= 0 :
            raise ValueError("deviation should be greater than zero")
        if not isinstance(deviation,int) :
            raise TypeError("deviation must be an integer")
        if expiry_date == None:
            expiry_date = self.get_nearest_expiry(symbol)
        else :
            if not isinstance(expiry_date,datetime):
                raise TypeError("expiry_date must be a datetime object format")
        
        contract = self.get_stored_master_data("NFO")
            
        if symbol not in self.strike_diff :
            self.strike_diff.update({symbol:self.get_strike_diff(symbol)})
        strike_diff = self.strike_diff[symbol]
        ATM = round(price/strike_diff) * strike_diff
        upper_strike = ATM + strike_diff*deviation
        lower_strike = ATM - strike_diff*deviation
        
        filter_contract = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == symbol)&(contract['Strike Price'] <= upper_strike)&(contract['Strike Price'] >= lower_strike)&(contract['Expiry Date'] == expiry_date.strftime('%Y-%m-%d'))]
        
        if len(filter_contract) == 0:
            return self._error_response("No Data")
        else:
            inst=[]
            filter_contract = filter_contract.reset_index()
            for i in range(len(filter_contract)):
                inst.append(Instrument(filter_contract['Exch'][i], filter_contract['Token'][i], filter_contract['Symbol'][i], filter_contract['Trading Symbol'][i], filter_contract['Expiry Date'][i],filter_contract['Lot Size'][i]))
            return inst

    def __get_specificType_opt_inst_n_deviation(self,symbol,price,deviation,opType,expiry_date=None):
        if deviation <= 0 :
            raise ValueError("deviation should be greater than zero")
        if not isinstance(deviation,int) :
            raise TypeError("deviation must be an integer")
        if expiry_date == None:
            expiry_date = self.get_nearest_expiry(symbol)
        else :
            if not isinstance(expiry_date,datetime):
                raise TypeError("expiry_date must be a datetime object format")
        
        contract = self.get_stored_master_data("NFO")
            
        if symbol not in self.strike_diff :
            self.strike_diff.update({symbol:self.get_strike_diff(symbol)})
        strike_diff = self.strike_diff[symbol]
        ATM = round(price/strike_diff) * strike_diff
        upper_strike = ATM + strike_diff*deviation
        lower_strike = ATM - strike_diff*deviation
        
        filter_contract = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == symbol)&(contract['Strike Price'] <= upper_strike)&(contract['Strike Price'] >= lower_strike)&(contract['Option Type'] == opType)&(contract['Expiry Date'] == expiry_date.strftime('%Y-%m-%d'))]
        
        if len(filter_contract) == 0:
            return self._error_response("No Data")
        else:
            inst=[]
            filter_contract = filter_contract.reset_index()
            for i in range(len(filter_contract)):
                inst.append(Instrument(filter_contract['Exch'][i], filter_contract['Token'][i], filter_contract['Symbol'][i], filter_contract['Trading Symbol'][i], filter_contract['Expiry Date'][i],filter_contract['Lot Size'][i]))
            return inst


    def get_CE_opt_inst_n_deviation(self,symbol,price,deviation,expiry_date=None) :
        return self.__get_specificType_opt_inst_n_deviation(symbol,price,deviation,"CE",expiry_date)
        
    def get_PE_opt_inst_n_deviation(self,symbol,price,deviation,expiry_date=None) :
        return self.__get_specificType_opt_inst_n_deviation(symbol,price,deviation,"PE",expiry_date)
        
    def get_nearest_expiry(self, symbol,n=0) :
        return self.get_option_expiry_list_by_symbol(symbol)[n]
    
    def get_option_expiry_list_by_symbol(self, symbol) :
        contract = self.get_stored_master_data("NFO")
        
        filter_contract = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == symbol)&(contract['Instrument Type'] == 'OPTIDX')&(contract['Option Type'] != 'XX')]['Expiry Date'].values.tolist()
        filter_contract = list(set(filter_contract))
        filter_contract = [datetime.strptime(sel_date, '%Y-%m-%d') for sel_date in filter_contract]
        filter_contract.sort()

        return filter_contract
    
    def get_index_option_expiry_list(self):
        contract = self.get_stored_master_data("NFO")
        # contract['Expiry Date'] = pd.to_datetime(contract['Expiry Date'],format="%Y-%m-%d")
        filter_contract = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == 'NIFTY')&(contract['Instrument Type'] == 'OPTIDX')]['Expiry Date'].values.tolist()
        filter_contract = list(set(filter_contract))
        filter_contract = [datetime.strptime(sel_date, '%Y-%m-%d') for sel_date in filter_contract]
        filter_contract.sort()

        return filter_contract
        
    def get_fut_expiry_list(self):
        contract = self.get_stored_master_data("NFO")
        # contract['Expiry Date'] = pd.to_datetime(contract['Expiry Date'],format="%Y-%m-%d")
        filter_contract = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == 'NIFTY')&(contract['Instrument Type'] == 'FUTIDX')]['Expiry Date'].values.tolist()
        filter_contract = list(set(filter_contract))
        filter_contract = [datetime.strptime(sel_date, '%Y-%m-%d') for sel_date in filter_contract]
        filter_contract.sort()

        return filter_contract
    
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
        
        gcd = gcd / 100
        
        return gcd
    
    def get_strike_list(self,symbol) :
        contract = self.get_stored_master_data("NFO")
                
        strike_list = contract[(contract['Exch'] == "NFO")&(contract['Symbol'] == symbol)&(contract['Option Type'] != 'XX')]['Strike Price'].unique()
        strike_list.sort()
        strike_list = [float(strike) for strike in strike_list]
        return strike_list
    
    
    
    def generate_option_diff(self,symbol_list=None) :

        contract = self.get_stored_master_data("NFO")
                
        if symbol_list == None :
            symbol_list = contract[(contract['Exch'] == "NFO")].Symbol.unique()

        for symbol in symbol_list:
            self.strike_diff.update({symbol:self.get_strike_diff(symbol)})
        
        
        
    def getHistoricalData(self,instrument,resolution,_from,_to) :
        if(type(resolution) is not Resolution):
            raise TypeError("Required parameter resolution not of type Resolution")
        
        if not isinstance(instrument, Instrument):
            raise TypeError("Required parameter instrument not of type Instrument")
        if resolution == Resolution.DAY :
            res = "D"
        elif resolution == Resolution.MINUTE :
            res = "M"
        data = {
            "token" : str(instrument.token),
            "resolution" : res,
            "from" : _from,
            "to" : _to,
            "exchange" : instrument.exchange
        }
        return self._post("historicaldata",data=data)

    def invalid_sess(self,session_ID):
        url = self.base_url + 'ws/invalidateSocketSess'
        headers = {
            'Authorization': 'Bearer ' + self.user_id + ' ' + session_ID,
            'Content-Type': 'application/json'
        }
        payload = {"loginType": "API"}
        datas = json.dumps(payload)
        response = requests.request("POST", url, headers=headers, data=datas)
        return response.json()

    def createSession(self,session_ID):
        url = self.base_url + 'ws/createSocketSess'
        headers = {
            'Authorization': 'Bearer ' + self.user_id + ' ' + session_ID,
            'Content-Type': 'application/json'
        }
        payload = {"loginType": "API"}
        datas = json.dumps(payload)
        response = requests.request("POST", url, headers=headers, data=datas)

        return response.json()

    def __modify_human_readable_values(self,_dictionary) :
        mapper = {
            'ft' : "feed_time",
            'e' : "exchange",
            'tk' : "token",
            'lp' : "ltp",
            'pc' : "percChg",
            'cv' : "absChg",
            'v' : 'volume',
            'o' : 'open',
            'h' : 'high',
            'l' : 'low',
            'c' : 'close',
            "ap" : "avgPrc",
            'ts' : "symbolName",
            'uc' : "upperCircuit",
            'lc' : "lowerCircuit",
        }
        convert_float_list = ['lp','pc','cv','v','o','h','l','c','ap','uc','lc','oi','ltq','tbq','tsq']
        
        new_dict = {}

        for _key in _dictionary.keys():
            if _key in convert_float_list:
                result = float(_dictionary[_key])
                if _key == 'v' :
                    result = int(result)
            elif _key == 'ft' :
                result = datetime.fromtimestamp(int(_dictionary['ft']))
            else :
                result = _dictionary[_key]
                
            if _key in mapper.keys() :
                new_dict[mapper[_key]] = result
            else :
                new_dict[_key] = result
                
        return new_dict
    def on_message(self,ws, message):
        data = json.loads(message)

        if data['t'] == 'ck' :
            print("Websocket Connection established")
            if self.__connection_acknowledgement_callback :
                self.__connection_acknowledgement_callback()
        
        if data['t'] == 'tk' :
            if self.__on_market_subscribe_callback :
                res = self.__modify_human_readable_values(data) 
                self.__on_market_subscribe_callback(res)
        
        if data['t'] == 'tf' :
            if self.__on_market_feed_tick_callback :
                res = self.__modify_human_readable_values(data) 
                self.__on_market_feed_tick_callback(res)
        
        if data['t'] == 'dk':
            if self.__on_depth_subscribe_callback :
                res = self.__modify_human_readable_values(data) 
                self.__on_depth_subscribe_callback(res)
        
        if data['t'] == 'df' :
            if self.__on_depth_feed_tick_callback :
                res = self.__modify_human_readable_values(data) 
                self.__on_depth_feed_tick_callback(res)

        # if True :
        #     if 'ft' not in data :
        #         print('ft not there')
        #         print()
        #     else :
        #         print("time ",datetime.fromtimestamp(int(data['ft'])))
            
        #     print(data)
            
        # if 's' in data and data['s'] == 'OK':
        #     self.ws_connection =True

    def __on_error_callback(self,ws, error):
        print("__on_error_callback  ",error)

    def __on_close_callback(self,ws, close_status_code, close_msg):
        self.__websocket_connected = False
        print("Websocket connection is closed! Reason:%s"%close_msg)
        self.ws_connection = False

    def __on_open_callback(self,ws):
        print("sdjfvljl")
        self.__websocket_connected = True
        initCon = {
            "susertoken": self.ENC,
            "t": "c",
            "actid": self.user_id + "_API",
            "uid": self.user_id + "_API",
            "source": "API"
        }
        self.__websocket.send(json.dumps(initCon))

        if self.__on_open :
            self.__on_open()
            
        

    def search_instruments(self,exchange,symbol):
        base_url=self.base_url.replace('/AliceBlueAPIService/api','')
        scrip_Url = base_url+"DataApiService/v2/exchange/getScripForSearch"
        # print(scrip_Url)
        data = {'symbol':symbol, 'exchange': [exchange]}
        # print(data)
        scrip_response = self._dummypost(scrip_Url, data)
        if scrip_response ==[]:
            return self._error_response('Symbol not found')
        else:
            inst=[]
            for i in range(len(scrip_response)):
                # print(scrip_response[i])
                inst.append(Instrument(scrip_response[i]['exch'],scrip_response[i]['token'],scrip_response[i]['formattedInsName'],scrip_response[i]['symbol'],'',''))
            return inst

    def subscribe(self, instrument, live_feed_type):
        """ subscribe to the current feed of an instrument """
        if(type(live_feed_type) is not LiveFeedType):
            raise TypeError("Required parameter live_feed_type not of type LiveFeedType")
        arr = []
        print("hi")
        if (isinstance(instrument, list)):
            for _instrument in instrument:
                if not isinstance(_instrument, Instrument):
                    raise TypeError("Required parameter instrument not of type Instrument")
                arr.append(('|').join([_instrument.exchange, str(_instrument.token)]))
                self.__subscribers[_instrument] = live_feed_type
        else:
            if not isinstance(instrument, Instrument):
                raise TypeError("Required parameter instrument not of type Instrument")
            arr = [('|').join([instrument.exchange, str(instrument.token)])]
            self.__subscribers[instrument] = live_feed_type
        
        arr = ('#').join(arr)
        if(live_feed_type == LiveFeedType.MARKET):
            mode = 't' 
            logger.debug("Subscribed to {} of type Market".format(arr))
            print("Subscribed to {} of type Market".format(arr))
        elif(live_feed_type == LiveFeedType.DEPTH):
            mode = 'd' 
            logger.debug("Subscribed to {} of type Depth".format(arr))
            print("Subscribed to {} of type Depth".format(arr))
        data = json.dumps({'k' : arr, 't' : mode})
        return self.__ws_send(data)

    # Uunsibscribe is not working properly , instruments are not unsubscribed
    def unsubscribe(self, instrument, live_feed_type):
        """ unsubscribe to the current feed of an instrument """
        if(type(live_feed_type) is not LiveFeedType):
            raise TypeError("Required parameter live_feed_type not of type LiveFeedType")
        arr = []
        print("hi")
        if (isinstance(instrument, list)):
            for _instrument in instrument:
                print(_instrument)
                if not isinstance(_instrument, Instrument):
                    raise TypeError("Required parameter instrument not of type Instrument")
                arr.append(('|').join([_instrument.exchange, str(_instrument.token)]))
                self.__subscribers[_instrument] = live_feed_type
        else:
            if not isinstance(instrument, Instrument):
                raise TypeError("Required parameter instrument not of type Instrument")
            arr = [('|').join([instrument.exchange, str(instrument.token)])]
            self.__subscribers[instrument] = live_feed_type
        
        arr = ('#').join(arr)
        
        if(live_feed_type == LiveFeedType.MARKET):
            mode = 'u' 
            logger.debug("Unsubscribed to {} of type Market".format(arr))
            print("Unsubscribed to {} of type Market".format(arr))
        elif(live_feed_type == LiveFeedType.DEPTH):
            mode = 'ud' 
            logger.debug("Unsubscribed to {} of type Depth".format(arr))
            print("Unsubscribed to {} of type Depth".format(arr))
        data = json.dumps({'k' : arr, 't' : mode})
        return self.__ws_send(data)

    def __ws_run_forever(self):
        while self.__keep_running:
            try:
                self.__websocket.run_forever()
            except Exception as e:
                logger.warning(f"websocket run forever ended in exception, {e}")
            sleep(0.1) # Sleep for 100ms between reconnection.

    def __ws_send(self, *args, **kwargs):
        while self.__websocket_connected == False:
            sleep(0.05)  # sleep for 50ms if websocket is not connected, wait for reconnection
        with self.__ws_mutex:
            ret = self.__websocket.send(*args, **kwargs)
        return ret

    def start_websocket(self, subscribe_callback = None, 
                                order_update_callback = None,
                                socket_open_callback = None,
                                socket_close_callback = None,
                                socket_error_callback = None,
                                run_in_background=False,
                                market_status_messages_callback = None,
                                exchange_messages_callback = None,
                                oi_callback = None,
                                dpr_callback = None,
                                websocket_conn_callback= None,
                                mkt_subscribe_callback = None,
                                mkt_tick_callback = None,
                                dpt_subscribe_callback = None,
                                dpt_tick_callback = None):
        """ Start a websocket connection for getting live data """
        # print("Starting websocket connection ajsdbckdbkdb")
        # print(socket_open_callback)
        self.__on_open = socket_open_callback
        self.__on_disconnect = socket_close_callback
        self.__on_error = socket_error_callback
        self.__subscribe_callback = subscribe_callback
        self.__order_update_callback = order_update_callback
        self.__market_status_messages_callback = market_status_messages_callback
        self.__exchange_messages_callback = exchange_messages_callback
        self.__oi_callback = oi_callback
        self.__dpr_callback = dpr_callback

        self.__connection_acknowledgement_callback =  websocket_conn_callback
        self.__on_market_subscribe_callback = mkt_subscribe_callback
        self.__on_market_feed_tick_callback =  mkt_tick_callback
        self.__on_depth_subscribe_callback =  dpt_subscribe_callback
        self.__on_depth_feed_tick_callback =  dpt_tick_callback

        session_request=self.session_id
        if session_request:
            session_id = session_request
            sha256_encryption1 = hashlib.sha256(session_id.encode('utf-8')).hexdigest()
            self.ENC = hashlib.sha256(sha256_encryption1.encode('utf-8')).hexdigest()
            invalidSess = self.invalid_sess(session_id)
            
            if invalidSess['stat']=='Ok':
                print("STAGE 1: Invalidate the previous session :",invalidSess['stat'])
                createSess = self.createSession(session_id)
                if createSess['stat']=='Ok':
                    print("STAGE 2: Create the new session :", createSess['stat'])
                    print("Connecting to Socket ...")
                    # websocket.enableTrace(False)
                    self.__websocket = websocket.WebSocketApp(self._sub_urls['base_url_socket'],
                                                on_open=self.__on_open_callback,
                                                on_message=self.on_message,
                                                on_close=self.__on_close_callback,
                                                on_error=self.__on_error_callback)

                                                
                    # th = threading.Thread(target=self.__send_heartbeat)
                    # th.daemon = True
                    # th.start()
                    self.__websocket.keep_running = True 
                    if run_in_background is True:
                        self.__ws_thread = threading.Thread(target=self.__ws_run_forever)
                        self.__ws_thread.daemon = True
                        self.__ws_thread.start()
                    else:
                        self.__ws_run_forever()
            
    
    
class Alice_Wrapper():
    def open_net_position(Net_position):
        open_net_position = [data for data in Net_position if data['Netqty'] != '0']
        return open_net_position

    def close_net_poition(Net_position):
        close_net_position = [data for data in Net_position if data['Netqty'] == '0']
        return close_net_position

    def subscription(script_list):
        if len(script_list) > 0:
            Aliceblue.script_subscription_instrument = script_list
            sub_prams=''
            # print(script_list)
            for i in range(len(script_list)):
                end_point = '' if i == len(script_list)-1 else '#'
                sub_prams=sub_prams+script_list[i].exchange+'|'+str(script_list[i].token)+end_point
            return sub_prams
        else:
            return {'stat':'Not_ok','emsg':'Script response is not fetched properly. Please check once'}

    def order_history(response_data):
        if response_data:
            old_response_data=[]
            for new_json in response_data:
                old_json = {
                    "validity": new_json['Validity'],
                    "trigger_price": new_json['Trgprc'],
                    "transaction_type": new_json['Trantype'],
                    "trading_symbol": new_json['Trsym'],
                    "rejection_reason": new_json['RejReason'],
                    "quantity": new_json['Qty'],
                    "product": new_json['Pcode'],
                    "price_to_fill": new_json['Prc'],
                    "order_status": new_json['Status'],
                    "oms_order_id": new_json['Nstordno'],
                    "nest_request_id": new_json['RequestID'],
                    "filled_quantity": new_json['Fillshares'],
                    "exchange_time": new_json['orderentrytime'],
                    "exchange_order_id": new_json['ExchOrdID'],
                    "exchange": new_json['Exchange'],
                    "disclosed_quantity": new_json['Dscqty'],
                    "client_id": new_json['user'],
                    "average_price": new_json['Avgprc'],
                    "order_tag":new_json['Remarks']
                }
                old_response_data.append(old_json)
            return old_response_data

    def get_balance(response):
        print(len(response),'stat' not in response)
        cash_pos=[]
        for i in range(len(response)):
            data={
                            "utilized": {
                                "var_margin": response[i]['varmargin'],
                                "unrealised_m2m": response[i]['unrealizedMtomPrsnt'],
                                "span_margin": response[i]['spanmargin'],
                                "realised_m2m": response[i]['realizedMtomPrsnt'],
                                "premium_present": response[i]['premiumPrsnt'],
                                "pay_out": response[i]['payoutamount'],
                                "multiplier": response[i]['multiplier'],
                                "exposure_margin": response[i]['exposuremargin'],
                                "elm": response[i]['elm'],
                                "debits": response[i]['debits']
                            },
                            "segment": response[i]['segment'],
                            "net": response[i]['net'],
                            "category": response[i]['category'],
                            "available": {
                                "pay_in": response[i]['rmsPayInAmnt'],
                                "notionalCash": response[i]['notionalCash'],
                                "direct_collateral_value": response[i]['directcollateralvalue'],
                                "credits": response[i]['credits'],
                                "collateral_value": response[i]['collateralvalue'],
                                "cashmarginavailable": response[i]['cashmarginavailable'],
                                "adhoc_margin": response[i]['adhocMargin']
                            }
                        }
            cash_pos.append(data)
        if 'stat' not in response:
            old_response = {
                "status": "success",
                "message": "",
                "data": {
                    "cash_positions": cash_pos
                }
            }
            return old_response
        else:
            return response

    def get_profile(response):
        if 'stat' not in response:
            exch = response['exchEnabled']
            exch_enabled = []
            if '|' in exch:
                exchange = exch.split('|')
                for ex in exchange:
                    data = ex.split('_')[0].upper()
                    if data != '':
                        exch_enabled.append(data)
            else:
                exch_enabled.append(exch.split('_')[0].upper())
            old_response = {
                "status": "success",
                "message": "",
                "data": {
                    "phone": response['cellAddr'],
                    "pan_number": "",
                    "name": response['accountName'],
                    "login_id": response['accountId'],
                    "exchanges": exch_enabled,
                    "email_address": response['emailAddr'],
                    "dp_ids": [],
                    "broker_name": "ALICEBLUE",
                    "banks": [],
                    "backoffice_enabled": None
                }
            }
            return old_response
        else:
            return response




