import sys
import json
import os
from loguru import logger
from threading import Thread
from filelock import FileLock, Timeout
from typing import Optional, Callable
from datetime import datetime, timedelta, time
from wtpy.apps.datahelper.DHDefs import BaseDataHelper, DBHelper
from wtpy.WtCoreDefs import WTSBarStruct
from datetime import datetime
from xtquant import (
    xtdata,
    xtconstant,
    xtdatacenter as xtdc
)
from xtquant import xtconstant
from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
from xtquant.xttype import (
    StockAccount,
    XtAsset,
    XtOrder,
    XtPosition,
    XtTrade,
    XtOrderResponse,
    XtCancelOrderResponse,
    XtOrderError,
    XtCancelError
)
from wtpy.WtUtility import (
    get_file_path,
    round_to
)
from pandas import DataFrame
from wtpy.WtObject import (
    Exchange,
    Interval,
    SubscribeRequest,
    HistoryRequest,
    TickData,
    BarData,
    TradeData,
    OrderData,
    OptionType,
    Status,
    Direction,
    OrderType,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    Offset,
    MarketData
)
from wtpy.WtConstant import (
    Exchange,
    Product,
    Period,
    Dividend,
    MarketsSector
)
from wtpy.event.engine import EventEngine, EVENT_TIMER

from dataclasses import dataclass
if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo, available_timezones              # noqa
else:
    from backports.zoneinfo import ZoneInfo, available_timezones    # noqa

INTERVAL_VT2XT: dict[Interval, str] = {
    Interval.MINUTE: "1m",
    Interval.DAILY: "1d",
    Interval.TICK: "tick"
}

PERIOD_WT2XT: dict[Period, str] = {
    Period.MINUTE_15: Period.MINUTE_15.value,
    Period.MINUTE_30: Period.MINUTE_15.value,
    Period.MINUTE_60: Period.MINUTE_15.value,
    Period.WEEKLY: Period.DAILY.value,
    Period.MONTHLY: Period.DAILY.value,
    Period.YEARLY: Period.DAILY.value,
}

INTERVAL_ADJUSTMENT_MAP: dict[Interval, timedelta] = {
    Interval.MINUTE: timedelta(minutes=1),
    Interval.DAILY: timedelta()         # 日线无需进行调整
}

# 交易所映射
EXCHANGE_VT2XT: dict[Exchange, str] = {
    Exchange.SSE: "SH",
    Exchange.SZSE: "SZ",
    Exchange.BSE: "BJ",
    Exchange.SHFE: "SF",
    Exchange.CFFEX: "IF",
    Exchange.INE: "INE",
    Exchange.DCE: "DF",
    Exchange.CZCE: "ZF",
    Exchange.GFEX: "GF",
}

EXCHANGE_XT2VT: dict[str, Exchange] = {v: k for k, v in EXCHANGE_VT2XT.items()}
EXCHANGE_XT2VT["SHO"] = Exchange.SSE
EXCHANGE_XT2VT["SZO"] = Exchange.SZSE


# 委托状态映射
STATUS_XT2VT: dict[str, Status] = {
    xtconstant.ORDER_UNREPORTED: Status.SUBMITTING,
    xtconstant.ORDER_WAIT_REPORTING: Status.SUBMITTING,
    xtconstant.ORDER_REPORTED: Status.NOTTRADED,
    xtconstant.ORDER_REPORTED_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PARTSUCC_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_PART_CANCEL: Status.CANCELLED,
    xtconstant.ORDER_CANCELED: Status.CANCELLED,
    xtconstant.ORDER_PART_SUCC: Status.PARTTRADED,
    xtconstant.ORDER_SUCCEEDED: Status.ALLTRADED,
    xtconstant.ORDER_JUNK: Status.REJECTED
}

# 多空方向映射
DIRECTION_VT2XT: dict[tuple, str] = {
    (Direction.LONG, Offset.NONE): xtconstant.STOCK_BUY,
    (Direction.SHORT, Offset.NONE): xtconstant.STOCK_SELL,
    (Direction.LONG, Offset.OPEN): xtconstant.STOCK_OPTION_BUY_OPEN,
    (Direction.LONG, Offset.CLOSE): xtconstant.STOCK_OPTION_BUY_CLOSE,
    (Direction.SHORT, Offset.OPEN): xtconstant.STOCK_OPTION_SELL_OPEN,
    (Direction.SHORT, Offset.CLOSE): xtconstant.STOCK_OPTION_SELL_CLOSE,
}
DIRECTION_XT2VT: dict[str, tuple] = {v: k for k, v in DIRECTION_VT2XT.items()}

POSDIRECTION_XT2VT: dict[int, Direction] = {
    xtconstant.DIRECTION_FLAG_BUY: Direction.LONG,
    xtconstant.DIRECTION_FLAG_SELL: Direction.SHORT
}

# 委托类型映射
ORDERTYPE_VT2XT: dict[tuple, int] = {
    (Exchange.SSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.SZSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
    (Exchange.BSE, OrderType.LIMIT): xtconstant.FIX_PRICE,
}
ORDERTYPE_XT2VT: dict[int, OrderType] = {
    50: OrderType.LIMIT,
}

CHINA_TZ = ZoneInfo("Asia/Shanghai")

# 合约数据全局缓存字典
symbol_contract_map: dict[str, ContractData] = {}

def stdCodeToTQ(stdCode:str):
    items = stdCode.split(".")
    exchg = items[0]
    if len(items) == 2 and exchg in ['SSE', 'SZSE']:
        # 简单股票代码，格式如SSE.600000
        return stdCode
    elif items[1] in ["IDX","ETF","STK","OPT"]:
        # 标准股票代码，格式如SSE.IDX.000001
        return exchg + "." + items[2]
    elif len(items) == 3 and exchg in ["SHFE", "CFFEX", "DCE", "CZCE", "INE", "GFEX"]:
        # 标准期货代码，格式如CFFEX.IF.2103
        if items[2] != 'HOT':
            return exchg + '.' + items[1] + items[2]
        else:
            return "KQ.m@" + exchg + '.' +items[1]
    else:
        return stdCode

def generate_datetime(timestamp: int, millisecond: bool = True) -> datetime:
    """生成本地时间"""
    if millisecond:
        dt: datetime = datetime.fromtimestamp(timestamp / 1000)
    else:
        dt: datetime = datetime.fromtimestamp(timestamp)
    dt: datetime = dt.replace(tzinfo=CHINA_TZ)
    return dt

def process_etf_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> Optional[ContractData]:
    """处理ETF期权"""
    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 筛选期权合约合约（ETF期权代码为8位）
    if len(symbol) != 8:
        return None

    # 查询转换数据
    data: dict = get_instrument_detail(xt_symbol, True)

    name: str = data["InstrumentName"]
    if "购" in name:
        option_type = OptionType.CALL
    elif "沽" in name:
        option_type = OptionType.PUT
    else:
        return None

    if "A" in name:
        option_index = str(data["OptExercisePrice"]) + "-A"
    else:
        option_index = str(data["OptExercisePrice"]) + "-M"

    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_portfolio=data["OptUndlCode"] + "_O",
        option_index=option_index,
        option_type=option_type,
        option_underlying=data["OptUndlCode"] + "-" + str(data["ExpireDate"])[:6],
        gateway_name=gateway_name
    )

    return contract


def process_futures_option(get_instrument_detail: Callable, xt_symbol: str, gateway_name: str) -> Optional[ContractData]:
    """处理期货期权"""
    # 筛选期权合约
    data: dict = get_instrument_detail(xt_symbol, True)

    option_strike: float = data["OptExercisePrice"]
    if not option_strike:
        return None

    # 拆分XT代码
    symbol, xt_exchange = xt_symbol.split(".")

    # 移除产品前缀
    for ix, w in enumerate(symbol):
        if w.isdigit():
            break

    suffix: str = symbol[ix:]

    # 过滤非期权合约
    if "(" in symbol or " " in symbol:
        return None

    # 判断期权类型
    if "C" in suffix:
        option_type = OptionType.CALL
    elif "P" in suffix:
        option_type = OptionType.PUT
    else:
        return None

    # 获取期权标的
    if "-" in symbol:
        option_underlying: str = symbol.split("-")[0]
    else:
        option_underlying: str = data["OptUndlCode"]

    # 转换数据
    contract: ContractData = ContractData(
        symbol=data["InstrumentID"],
        exchange=EXCHANGE_XT2VT[xt_exchange],
        name=data["InstrumentName"],
        product=Product.OPTION,
        size=data["VolumeMultiple"],
        pricetick=data["PriceTick"],
        min_volume=data["MinLimitOrderVolume"],
        option_strike=data["OptExercisePrice"],
        option_listed=datetime.strptime(data["OpenDate"], "%Y%m%d"),
        option_expiry=datetime.strptime(data["ExpireDate"], "%Y%m%d"),
        option_index=str(data["OptExercisePrice"]),
        option_type=option_type,
        option_underlying=option_underlying,
        gateway_name=gateway_name
    )

    if contract.exchange == Exchange.CZCE:
        contract.option_portfolio = data["ProductID"][:-1]
    else:
        contract.option_portfolio = data["ProductID"]

    return contract


def get_history_df(req: HistoryRequest, output: Callable = print) -> DataFrame:
    """获取历史数据DataFrame"""
    symbol: str = req.symbol
    exchange: Exchange = req.exchange
    start: datetime = req.start
    end: datetime = req.end
    interval: Interval = req.interval

    if not interval:
        interval = Interval.TICK

    xt_interval: str = INTERVAL_VT2XT.get(interval, None)
    if not xt_interval:
        output(f"迅投研查询历史数据失败：不支持的时间周期{interval.value}")
        return DataFrame()

    # 为了查询夜盘数据
    end += timedelta(1)

    # 从服务器下载获取
    xt_symbol: str = symbol + "." + EXCHANGE_VT2XT[exchange]
    start: str = start.strftime("%Y%m%d%H%M%S")
    end: str = end.strftime("%Y%m%d%H%M%S")

    if exchange in (Exchange.SSE, Exchange.SZSE) and len(symbol) > 6:
        xt_symbol += "O"

    xtdata.download_history_data(xt_symbol, xt_interval, start, end)
    data: dict = xtdata.get_local_data([], [xt_symbol], xt_interval, start, end, -1, "front_ratio", False)      # 默认等比前复权

    df: DataFrame = data[xt_symbol]
    return df


class DHQmtSdk(BaseDataHelper):
    """
    WonderTrader用于对接迅投研的实时行情接口。
    """

    default_name: str = "XT"

    default_setting: dict[str, str] = {
        "token": "",
        "股票市场": ["是", "否"],
        "期货市场": ["是", "否"],
        "期权市场": ["是", "否"],
        "仿真交易": ["是", "否"],
        "账号类型": ["股票", "股票期权"],
        "QMT路径": "",
        "资金账号": ""
    }

    exchanges: list[str] = list(EXCHANGE_VT2XT.keys())

    lock_filename = "xt_lock"
    lock_filepath = get_file_path(lock_filename)
    def __init__(self):
        BaseDataHelper.__init__(self)
        self.username = None
        self.password = None
        self.lock: FileLock = None
        self.inited: bool = False
        self.token: str = ""
        self.stock_active: bool = False
        self.futures_active: bool = False
        self.option_active: bool = False
        self.md_api: "XtMdApi" = XtMdApi(self, "DHXtSdk")
        self.td_api: "XtTdApi" = XtTdApi()

        self.trading: bool = False
        self.orders: dict[str, OrderData] = {}

        self.stock_contracts: Dict[str, ContractData] = {}

        self.thread: Thread = None
        logger.info("XtSdk helper has been created.")
        return

    def auth(self, **kwargs):
        if self.isAuthed:
            return self.isAuthed
        
        self.username = kwargs["username"]
        self.password = kwargs["password"]
        self.token = kwargs["token"]
        self.stock_active = kwargs["stock_active"]
        self.futures_active = kwargs["futures_active"]
        self.option_active = kwargs["option_active"]
        
        suc = self.connect()

        self.isAuthed = suc
        
        return suc

    def dmpCodeListToFile(self, filename:str, hasIndex:bool=True, hasStock:bool=True):
        self.md_api.query_contracts()

    # 根据配置进行合约列表落地
    def dmpContractsToFileWithConfig(self):
        save_dir = "markets/"
        if not os.path.exists(save_dir):
            os.makedirs(save_dir)
        if self.stock_active:
            stock_markets: list = [
                MarketsSector.HSAG,
                MarketsSector.HSAG,
                MarketsSector.HSZZ,
                MarketsSector.HSETF,
                MarketsSector.HSZS,
                MarketsSector.JSAG
                
            ]
            for stock_m in stock_markets:
                self.dmpContractsToFile([MarketData(market_name=stock_m.value,file_name=save_dir + stock_m.name + ".json")])
            
        
        if self.futures_active:
            futures_markets: list = [
                MarketsSector.ZJSQH,
                MarketsSector.SQSQH,
                MarketsSector.NYZXQH,
                MarketsSector.DSYQH,
                MarketsSector.ZSSQH,
                MarketsSector.GQSQH
            ]
            for future_m in futures_markets:
                self.dmpContractsToFile([MarketData(market_name=future_m.value,file_name=save_dir + future_m.name + ".json")])

        if self.option_active:
            option_markets: list = [
                MarketsSector.SHQQ,
                MarketsSector.SZQQ,
                MarketsSector.ZJSQQ,
                MarketsSector.SQSQQ,
                MarketsSector.NYZXQQ,
                MarketsSector.DSSQQ,
                MarketsSector.ZSSQQ,
                MarketsSector.GQSQQ
                
            ]
            for option_m in option_markets:
                self.dmpContractsToFile([MarketData(market_name=option_m.value,file_name=save_dir + option_m.name + ".json")])
            
        
    def dmpContractsToFile(self, markets_sector: list):
        for mardata in markets_sector:
            contracts_data = self.md_api.query_contract(mardata.market_name)
            f = open(mardata.file_name, 'w', encoding='utf-8')
            f.write(json.dumps(contracts_data, sort_keys=True, indent=4, ensure_ascii=False))
            f.close()
            
        
    def on_stock_contract(self, contract: ContractData):
        self.stock_contracts[contract.vt_symbol] = contract
        
    def dmpAdjFactorsToFile(self, codes: list, filename: str):
        raise Exception("TqSdk has not Adj Factors api")

    def dmpAdjFactorsToDB(self, dbHelper: DBHelper, codes: list):
        raise Exception("TqSdk has not Adj Factors api")

    def dmpBarsToFile(self, folder:str, codes:list, start_date:datetime=None, end_date:datetime=None, period=Period.DAILY, dividend=Dividend.FRONT_RATIO):
        if start_date is None:
            start_time = ''
        else:
            start_time: str = start_date.strftime("%Y%m%d%H%M%S")

        if end_date is None:
            end_time = ''
        else:
            end_time: str = end_date.strftime("%Y%m%d%H%M%S")

        # 将传入的period转换为对应的下载周期
        download_period: str = PERIOD_WT2XT.get(period, None)
        if download_period is None:
            download_period = period.value
        
        
        # 下载历史数据到本地
        for stdCode in codes:
            xtdata.download_history_data(stock_code=stdCode, period=download_period, start_time=start_time, end_time=end_time)
        
        ############ 仅获取历史行情 #####################
        # subscribe = False # 设置订阅参数，使gmd_ex仅返回本地数据
        # count = -1 # 设置count参数，使gmd_ex返回全部数据
        ############ 仅获取最新行情 #####################
        # subscribe = True # 设置订阅参数，使gmd_ex仅返回最新行情
        # count = 1 # 设置count参数，使gmd_ex仅返回最新行情数据
        ############ 获取历史行情+最新行情 #####################
        # subscribe = True # 设置订阅参数，使gmd_ex仅返回最新行情
        # count = -1 # 设置count参数，使gmd_ex返回全部数据

        codes_data = xtdata.get_market_data_ex(field_list=[], stock_list=codes, period=period.value, start_time=start_time, end_time=end_time, count=-1)
        logger.info("codes_data %s..." % (codes_data))
        
        for stdCode in codes:
            code_df = codes_data[stdCode]
            code_detail = xtdata.get_instrument_detail(stdCode)
            # logger.info("df %s..." % (code_df))
            filename = "%s_%s.csv" % (stdCode, period.value)
            filepath = os.path.join(folder, filename)
            # logger.info("Writing bars into file %s..." % (filepath))
            code_df.to_csv(filepath)


    def dmpBarsToDB(self, dbHelper: DBHelper, codes: list, start_date: datetime = None, end_date: datetime = None,
                    period: str = "day"):
        raise Exception("TqSdk has not Adj Factors api")

    def dmpBars(self, codes:list, cb, start_date:datetime=None, end_date:datetime=None, period:str="day"):
        raise Exception("TqSdk has not Adj Factors api")


    def connect_xt_trader(self, setting: dict = default_setting) -> None:
        """连接交易接口"""
        if self.thread:
            return

        self.thread = Thread(target=self._connect_xt_trader, args=(setting,))
        self.thread.start()


        """初始化事件引擎，启动循环"""
        self.event_engine = EventEngine()
        self.event_engine.start()


    def _connect_xt_trader(self, setting: dict) -> None:
        """连接交易接口"""
        token: str = setting["token"]

        stock_active: bool = setting["股票市场"] == "是"
        futures_active: bool = setting["期货市场"] == "是"
        option_active: bool = setting["期权市场"] == "是"

        self.md_api.connect(token, stock_active, futures_active, option_active)

        self.trading = setting["仿真交易"] == "是"
        if self.trading:
            path: str = setting["QMT路径"] + "\\userdata"

            accountid: str = setting["资金账号"]

            if setting["账号类型"] == "股票":
                account_type: str = "STOCK"
            else:
                account_type: str = "STOCK_OPTION"

            self.td_api.connect(path, accountid, account_type)
            self.init_query()
    def connect(self, output: Callable = print) -> bool:
        """初始化"""
        logger.info("开始启动行情服务，请稍等")
        
        suc = self.md_api.connect(username=self.username, password=self.password, token=self.token, stock_active=self.stock_active, futures_active=self.futures_active, option_active=self.option_active)

        if suc:
            logger.info("行情接口连接成功")
        else:
            logger.info("行情接口连接失败")

        return suc

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        self.md_api.subscribe(req)

    def subscribe_whole_quote(self, code_list) -> None:
        """订阅全市场行情"""
        self.md_api.subscribe_whole_quote(code_list)

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        if self.trading:
            return self.td_api.send_order(req)
        else:
            return ""

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        if self.trading:
            self.td_api.cancel_order(req)

    def query_account(self) -> None:
        """查询资金"""
        if self.trading:
            self.td_api.query_account()

    def query_position(self) -> None:
        """查询持仓"""
        if self.trading:
            self.td_api.query_position()

    def query_history(self, req: HistoryRequest) -> None:
        """查询历史数据"""
        return None

    def on_order(self, order: OrderData) -> None:
        """推送委托数据"""
        self.orders[order.orderid] = order
        super().on_order(order)

    def get_order(self, orderid: str) -> OrderData:
        """查询委托数据"""
        return self.orders.get(orderid, None)

    def close(self) -> None:
        """关闭接口"""
        if self.trading:
            self.td_api.close()
    
    def process_timer_event(self, event) -> None:
        """定时事件处理"""
        self.count += 1
        if self.count < 2:
            return
        self.count = 0

        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)

    def init_query(self) -> None:
        """初始化查询任务,查询账户和持仓"""
        self.count: int = 0
        self.query_functions: list = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def on_tick(self, tick: TickData) -> None:
        """推送行情数据"""
        logger.info("on_tick: {}".format(tick))


class XtMdApi:
    """行情API"""

    lock_filename = "xt_lock"
    lock_filepath = get_file_path(lock_filename)

    def __init__(self, gateway, gateway_name) -> None:
        """构造函数"""

        self.inited: bool = False
        self.subscribed: set = set()

        self.token: str = ""
        self.stock_active: bool = False
        self.futures_active: bool = False
        self.option_active: bool = False
        self.gateway = gateway
        self.gateway_name = gateway_name

    def onMarketData(self, data: dict) -> None:
        '''
        tick - 分笔数据
        'time'                  #时间戳
        'lastPrice'             #最新价
        'open'                  #开盘价
        'high'                  #最高价
        'low'                   #最低价
        'lastClose'             #前收盘价
        'amount'                #成交总额
        'volume'                #成交总量
        'pvolume'               #原始成交总量
        'stockStatus'           #证券状态
        'openInt'               #持仓量
        'lastSettlementPrice'   #前结算
        'askPrice'              #委卖价
        'bidPrice'              #委买价
        'askVol'                #委卖量
        'bidVol'                #委买量
        'transactionNum'		#成交笔数
        '''
        """行情推送回调"""

        for xt_symbol, buf in data.items():
            # logger.info("onMarketData {}, {}",xt_symbol, buf)
            symbol, xt_exchange = xt_symbol.split(".")
            exchange = EXCHANGE_XT2VT[xt_exchange]
            logger.info(symbol, xt_exchange, exchange)
            tick: TickData = TickData(
                symbol=symbol,
                exchange=exchange,
                datetime=generate_datetime(buf["time"]),
                volume=buf["volume"],
                turnover=buf["amount"],
                open_interest=buf["openInt"],
                gateway_name=self.gateway_name
            )

            contract = symbol_contract_map.get(tick.vt_symbol, None)
            if contract is None:
                logger.warning("onMarketData: unknown xt_symbol {}".format(xt_symbol))
                continue
            
            tick.name = contract.name

            bp_data: list = buf["bidPrice"]
            ap_data: list = buf["askPrice"]
            bv_data: list = buf["bidVol"]
            av_data: list = buf["askVol"]

            tick.bid_price_1 = round_to(bp_data[0], contract.pricetick)
            tick.bid_price_2 = round_to(bp_data[1], contract.pricetick)
            tick.bid_price_3 = round_to(bp_data[2], contract.pricetick)
            tick.bid_price_4 = round_to(bp_data[3], contract.pricetick)
            tick.bid_price_5 = round_to(bp_data[4], contract.pricetick)

            tick.ask_price_1 = round_to(ap_data[0], contract.pricetick)
            tick.ask_price_2 = round_to(ap_data[1], contract.pricetick)
            tick.ask_price_3 = round_to(ap_data[2], contract.pricetick)
            tick.ask_price_4 = round_to(ap_data[3], contract.pricetick)
            tick.ask_price_5 = round_to(ap_data[4], contract.pricetick)

            tick.bid_volume_1 = bv_data[0]
            tick.bid_volume_2 = bv_data[1]
            tick.bid_volume_3 = bv_data[2]
            tick.bid_volume_4 = bv_data[3]
            tick.bid_volume_5 = bv_data[4]

            tick.ask_volume_1 = av_data[0]
            tick.ask_volume_2 = av_data[1]
            tick.ask_volume_3 = av_data[2]
            tick.ask_volume_4 = av_data[3]
            tick.ask_volume_5 = av_data[4]

            tick.last_price = round_to(buf["lastPrice"], contract.pricetick)
            tick.open_price = round_to(buf["open"], contract.pricetick)
            tick.high_price = round_to(buf["high"], contract.pricetick)
            tick.low_price = round_to(buf["low"], contract.pricetick)
            tick.pre_close = round_to(buf["lastClose"], contract.pricetick)

            self.gateway.on_tick(tick)

    def connect(
        self,
        username: str,
        password: str,
        token: str,
        stock_active: bool,
        futures_active: bool,
        option_active: bool
    ) -> None:
        """连接"""
        logger.info("开始启动行情服务，请稍等")

        self.username = username
        self.password = password
        self.token = token
        self.stock_active = stock_active
        self.futures_active = futures_active
        self.option_active = option_active

        if self.inited:
            logger.warning("行情接口已经初始化，请勿重复操作")
            return True

        try:
            # 使用Token连接，无需启动客户端
            if self.username != "client":
                self.init_xtdc()

            # 尝试查询合约信息，确认连接成功
            xtdata.get_instrument_detail("000001.SZ")
        except Exception as ex:
            logger.warning(f"迅投研数据服务初始化失败，发生异常：{ex}")
            return False

        self.inited = True

        logger.info("行情接口连接成功")

        return True

    def get_lock(self) -> bool:
        """获取文件锁，确保单例运行"""
        self.lock = FileLock(self.lock_filepath)

        try:
            self.lock.acquire(timeout=1)
            return True
        except Timeout:
            return False

    def init_xtdc(self) -> None:
        """初始化xtdc服务进程"""
        if not self.get_lock():
            return

        # 设置token
        xtdc.set_token(self.token)

        # 开启使用期货真实夜盘时间
        xtdc.set_future_realtime_mode(True)

        # 执行初始化，但不启动默认58609端口监听
        xtdc.init(False)

        # 设置监听端口58620
        xtdc.listen(port=58620)

    def query_contract(self, sector: str) -> list:
        names: list = xtdata.get_stock_list_in_sector(sector)
        return names
        
    def query_contracts(self) -> None:
        """查询合约信息"""
        if self.stock_active:
            self.query_stock_contracts()

        if self.futures_active:
            self.query_future_contracts()

        if self.option_active:
            self.query_option_contracts()

        logger.info("合约信息查询成功")

    def query_stock_contracts(self) -> None:
        """查询股票合约信息"""
        xt_symbols: list[str] = []
        markets: list = [
            "沪深A股",
            "沪深转债",
            "沪深ETF",
            "沪深指数",
            "京市A股"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            # 筛选需要的合约
            product = None
            symbol, xt_exchange = xt_symbol.split(".")

            if xt_exchange == "SZ":
                if xt_symbol.startswith("00"):
                    product = Product.EQUITY
                elif xt_symbol.startswith("159"):
                    product = Product.FUND
                else:
                    product = Product.INDEX
            elif xt_exchange == "SH":
                if xt_symbol.startswith(("60", "68")):
                    product = Product.EQUITY
                elif xt_symbol.startswith("51"):
                    product = Product.FUND
                else:
                    product = Product.INDEX
            elif xt_exchange == "BJ":
                product = Product.EQUITY

            if not product:
                continue

            # 生成并推送合约信息
            data: dict = xtdata.get_instrument_detail(xt_symbol)
            
            if data is None:
                logger.warning('get_instrument_detail failed! {}', xt_symbol)
                continue
            
            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                history_data=False,
                gateway_name=self.gateway_name
            )

            symbol_contract_map[contract.vt_symbol] = contract
            self.gateway.on_stock_contract(contract)

        logger.info('query_stock_contracts success')

    def query_future_contracts(self) -> None:
        """查询期货合约信息"""
        xt_symbols: list[str] = []
        markets: list = [
            "中金所期货",
            "上期所期货",
            "能源中心期货",
            "大商所期货",
            "郑商所期货",
            "广期所期货"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            logger.info("future {} names: {}", i, names)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            # 筛选需要的合约
            product = None
            symbol, xt_exchange = xt_symbol.split(".")

            if xt_exchange == "ZF" and len(symbol) > 6 and "&" not in symbol:
                product = Product.OPTION
            elif xt_exchange in ("IF", "GF") and "-" in symbol:
                product = Product.OPTION
            elif xt_exchange in ("DF", "INE", "SF") and ("C" in symbol or "P" in symbol) and "SP" not in symbol:
                product = Product.OPTION
            else:
                product = Product.FUTURES

            # 生成并推送合约信息
            if product == Product.OPTION:
                data: dict = xtdata.get_instrument_detail(xt_symbol, True)
            else:
                data: dict = xtdata.get_instrument_detail(xt_symbol)

            if not data["ExpireDate"]:
                if "00" not in symbol:
                    continue

            contract: ContractData = ContractData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                history_data=False,
                gateway_name=self.gateway_name
            )

            symbol_contract_map[contract.vt_symbol] = contract
            # self.gateway.on_contract(contract)

    def query_option_contracts(self) -> None:
        """查询期权合约信息"""
        xt_symbols: list[str] = []

        markets: list = [
            "上证期权",
            "深证期权",
            "中金所期权",
            "上期所期权",
            "能源中心期权",
            "大商所期权",
            "郑商所期权",
            "广期所期权"
        ]

        for i in markets:
            names: list = xtdata.get_stock_list_in_sector(i)
            logger.info("option {} names: {}", i, names)
            xt_symbols.extend(names)

        for xt_symbol in xt_symbols:
            ""
            _, xt_exchange = xt_symbol.split(".")

            if xt_exchange in {"SHO", "SZO"}:
                contract = process_etf_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)
            else:
                contract = process_futures_option(xtdata.get_instrument_detail, xt_symbol, self.gateway_name)

            if contract:
                symbol_contract_map[contract.vt_symbol] = contract
                # self.gateway.on_contract(contract)

    def query_bar_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[BarData]]:
        """查询K线数据"""
        history: list[BarData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        adjustment: timedelta = INTERVAL_ADJUSTMENT_MAP[req.interval]

        # 遍历解析
        auction_bar: BarData = None

        for tp in df.itertuples():
            # 将迅投研时间戳（K线结束时点）转换为VeighNa时间戳（K线开始时点）
            dt: datetime = datetime.fromtimestamp(tp.time / 1000)
            dt = dt.replace(tzinfo=CHINA_TZ)
            dt = dt - adjustment

            # 日线，过滤尚未走完的当日数据
            if req.interval == Interval.DAILY:
                incomplete_bar: bool = (
                    dt.date() == datetime.now().date()
                    and datetime.now().time() < time(hour=15)
                )
                if incomplete_bar:
                    continue
            # 分钟线，过滤盘前集合竞价数据（合并到开盘后第1根K线中）
            else:
                if (
                    req.exchange in (Exchange.SSE, Exchange.SZSE, Exchange.BSE, Exchange.CFFEX)
                    and dt.time() == time(hour=9, minute=29)
                ) or (
                    req.exchange in (Exchange.SHFE, Exchange.INE, Exchange.DCE, Exchange.CZCE, Exchange.GFEX)
                    and dt.time() in (time(hour=8, minute=59), time(hour=20, minute=59))
                ):
                    auction_bar = BarData(
                        symbol=req.symbol,
                        exchange=req.exchange,
                        datetime=dt,
                        open_price=float(tp.open),
                        volume=float(tp.volume),
                        turnover=float(tp.amount),
                        gateway_name="XT"
                    )
                    continue

            # 生成K线对象
            bar: BarData = BarData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                interval=req.interval,
                volume=float(tp.volume),
                turnover=float(tp.amount),
                open_interest=float(tp.openInterest),
                open_price=float(tp.open),
                high_price=float(tp.high),
                low_price=float(tp.low),
                close_price=float(tp.close),
                gateway_name="XT"
            )

            # 合并集合竞价数据
            if auction_bar:
                bar.open_price = auction_bar.open_price
                bar.volume += auction_bar.volume
                bar.turnover += auction_bar.turnover
                auction_bar = None

            history.append(bar)

        return history

    def query_tick_history(self, req: HistoryRequest, output: Callable = print) -> Optional[list[TickData]]:
        """查询Tick数据"""
        history: list[TickData] = []

        if not self.inited:
            n: bool = self.init(output)
            if not n:
                return history

        df: DataFrame = get_history_df(req, output)
        if df.empty:
            return history

        # 遍历解析
        for tp in df.itertuples():
            dt: datetime = datetime.fromtimestamp(tp.time / 1000)
            dt = dt.replace(tzinfo=CHINA_TZ)

            tick: TickData = TickData(
                symbol=req.symbol,
                exchange=req.exchange,
                datetime=dt,
                volume=float(tp.volume),
                turnover=float(tp.amount),
                open_interest=float(tp.openInt),
                open_price=float(tp.open),
                high_price=float(tp.high),
                low_price=float(tp.low),
                last_price=float(tp.lastPrice),
                pre_close=float(tp.lastClose),
                bid_price_1=float(tp.bidPrice[0]),
                ask_price_1=float(tp.askPrice[0]),
                bid_volume_1=float(tp.bidVol[0]),
                ask_volume_1=float(tp.askVol[0]),
                gateway_name="XT",
            )

            bid_price_2: float = float(tp.bidPrice[1])
            if bid_price_2:
                tick.bid_price_2 = bid_price_2
                tick.bid_price_3 = float(tp.bidPrice[2])
                tick.bid_price_4 = float(tp.bidPrice[3])
                tick.bid_price_5 = float(tp.bidPrice[4])

                tick.ask_price_2 = float(tp.askPrice[1])
                tick.ask_price_3 = float(tp.askPrice[2])
                tick.ask_price_4 = float(tp.askPrice[3])
                tick.ask_price_5 = float(tp.askPrice[4])

                tick.bid_volume_2 = float(tp.bidVol[1])
                tick.bid_volume_3 = float(tp.bidVol[2])
                tick.bid_volume_4 = float(tp.bidVol[3])
                tick.bid_volume_5 = float(tp.bidVol[4])

                tick.ask_volume_2 = float(tp.askVol[1])
                tick.ask_volume_3 = float(tp.askVol[2])
                tick.ask_volume_4 = float(tp.askVol[3])
                tick.ask_volume_5 = float(tp.askVol[4])

            history.append(tick)

        return history

    def subscribe(self, req: SubscribeRequest) -> None:
        """订阅行情"""
        if req.vt_symbol not in symbol_contract_map:
            return

        xt_exchange: str = EXCHANGE_VT2XT[req.exchange]
        if xt_exchange in {"SH", "SZ"} and len(req.symbol) > 6:
            xt_exchange += "O"

        xt_symbol: str = req.symbol + "." + xt_exchange

        if xt_symbol not in self.subscribed:
            xtdata.subscribe_quote(stock_code=xt_symbol, period="tick", callback=self.onMarketData)
            self.subscribed.add(xt_symbol)
    
    def subscribe_whole_quote(self, code_list) -> int:
        '''
        订阅全推数据 订阅后会首先返回当前最新的全推数据
        :param code_list: 市场代码列表 ["SH", "SZ"] 传入合约代码代表订阅指定的合约，示例：`['600000.SH', '000001.SZ']`
        :param callback:
            订阅回调函数onSubscribe(datas)
            :param datas: {stock1 : data1, stock2 : data2, ...} 数据字典
        :return: int 订阅序号 订阅成功返回`大于0`，失败返回`-1`
        '''
        subId = xtdata.subscribe_whole_quote(code_list=code_list, callback=self.onMarketData)
        if subId > 0:
            for code in code_list:
                self.subscribed.add(code)
        
        return subId

    def unsubscribe_quote(subId):
        xtdata.unsubscribe_quote(subId)

    def close(self) -> None:
        """关闭连接"""
        pass


class XtTdApi(XtQuantTraderCallback):
    """交易API"""

    def __init__(self):
        """构造函数"""
        super().__init__()

        self.inited: bool = False
        self.connected: bool = False

        self.account_id: str = ""
        self.path: str = ""
        self.account_type: str = ""

        self.order_count: int = 0

        self.active_localid_sysid_map: dict[str, str] = {}

        self.xt_client: XtQuantTrader = None
        self.xt_account: StockAccount = None

    def on_connected(self):
        """
        连接成功推送
        """
        logger.info("交易接口连接成功")

    def on_disconnected(self):
        """连接断开"""
        logger.info("交易接口连接断开，请检查与客户端的连接状态")
        self.connected = False

        # 尝试重连，重连需要更换session_id
        session: int = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)
        connect_result = self.connect(session)

        if connect_result:
            logger.warning("交易接口重连失败")
        else:
            logger.info("交易接口重连成功")

    def on_stock_trade(self, xt_trade: XtTrade) -> None:
        """成交变动推送"""
        if not xt_trade.order_remark:
            return

        symbol, xt_exchange = xt_trade.stock_code.split(".")

        direction, offset = DIRECTION_XT2VT.get(xt_trade.order_type, (None, None))
        if direction is None:
            return

        trade: TradeData = TradeData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_trade.order_remark,
            tradeid=xt_trade.traded_id,
            direction=direction,
            offset=offset,
            price=xt_trade.traded_price,
            volume=xt_trade.traded_volume,
            datetime=generate_datetime(xt_trade.traded_time, False),
            gateway_name=self.gateway_name
        )

        contract: ContractData = symbol_contract_map.get(trade.vt_symbol, None)
        if contract:
            trade.price = round_to(trade.price, contract.pricetick)

        # self.gateway.on_trade(trade)

    def on_stock_order(self, xt_order: XtOrder) -> None:
        """委托回报推送"""
        # 过滤非VeighNa Trader发出的委托
        if not xt_order.order_remark:
            return

        # 过滤不支持的委托类型
        type: OrderType = ORDERTYPE_XT2VT.get(xt_order.price_type, None)
        if not type:
            return

        direction, offset = DIRECTION_XT2VT.get(xt_order.order_type, (None, None))
        if direction is None:
            return

        symbol, xt_exchange = xt_order.stock_code.split(".")

        order: OrderData = OrderData(
            symbol=symbol,
            exchange=EXCHANGE_XT2VT[xt_exchange],
            orderid=xt_order.order_remark,
            direction=direction,
            offset=offset,
            type=type,                  # 目前测出来与文档不同，限价返回50，市价返回88
            price=xt_order.price,
            volume=xt_order.order_volume,
            traded=xt_order.traded_volume,
            status=STATUS_XT2VT.get(xt_order.order_status, Status.SUBMITTING),
            datetime=generate_datetime(xt_order.order_time, False),
            gateway_name=self.gateway_name
        )

        if order.is_active():
            self.active_localid_sysid_map[xt_order.order_remark] = xt_order.order_sysid
        else:
            self.active_localid_sysid_map.pop(xt_order.order_remark, None)

        contract: ContractData = symbol_contract_map.get(order.vt_symbol, None)
        if contract:
            order.price = round_to(order.price, contract.pricetick)

        # self.gateway.on_order(order)

    def on_query_order_async(self, xt_orders: list[XtOrder]) -> None:
        """委托信息异步查询回报"""
        if not xt_orders:
            return

        for data in xt_orders:
            self.on_stock_order(data)

        logger.info("委托信息查询成功")

    def on_query_asset_async(self, xt_asset: XtAsset) -> None:
        """资金信息异步查询回报"""
        if not xt_asset:
            return

        account: AccountData = AccountData(
            accountid=xt_asset.account_id,
            balance=xt_asset.total_asset,
            frozen=xt_asset.frozen_cash,
            gateway_name=self.gateway_name
        )
        account.available = xt_asset.cash

        # self.gateway.on_account(account)

    def on_query_trades_async(self, xt_trades: list[XtTrade]) -> None:
        """成交信息异步查询回报"""
        if not xt_trades:
            return

        for xt_trade in xt_trades:
            self.on_stock_trade(xt_trade)

        logger.info("成交信息查询成功")

    def on_query_positions_async(self, xt_positions: list[XtPosition]) -> None:
        """持仓信息异步查询回报"""
        if not xt_positions:
            return

        for xt_position in xt_positions:
            if self.account_type == "STOCK":
                direction: Direction = Direction.NET
            else:
                direction: Direction = POSDIRECTION_XT2VT.get(xt_position.direction, "")

            if not direction:
                continue

            symbol, xt_exchange = xt_position.stock_code.split(".")

            position: PositionData = PositionData(
                symbol=symbol,
                exchange=EXCHANGE_XT2VT[xt_exchange],
                direction=direction,
                volume=xt_position.volume,
                yd_volume=xt_position.can_use_volume,
                frozen=xt_position.volume - xt_position.can_use_volume,
                price=xt_position.open_price,
                gateway_name=self.gateway_name
            )

            # self.gateway.on_position(position)

    def on_order_error(self, xt_error: XtOrderError) -> None:
        """委托失败推送"""
        # order: OrderData = self.gateway.get_order(xt_error.order_remark)
        # if order:
        #     order.status = Status.REJECTED
        #     self.gateway.on_order(order)

        logger.error(f"交易委托失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_cancel_error(self, xt_error: XtCancelError) -> None:
        """撤单失败推送"""
        logger.error(f"交易撤单失败, 错误代码{xt_error.error_id}, 错误信息{xt_error.error_msg}")

    def on_order_stock_async_response(self, response: XtOrderResponse) -> None:
        """异步下单回报推送"""
        if response.error_msg:
            logger.error(f"委托请求提交失败：{response.error_msg}，本地委托号{response.order_remark}")
        else:
            logger.info(f"委托请求提交成功，本地委托号{response.order_remark}")

    def on_cancel_order_stock_async_response(self, response: XtCancelOrderResponse) -> None:
        """异步撤单回报推送"""
        if response.error_msg:
            logger.error(f"撤单请求提交失败：{response.error_msg}，系统委托号{response.order_sysid}")
        else:
            logger.info(f"撤单请求提交成功，系统委托号{response.order_sysid}")

    def connect(self, path: str, accountid: str, account_type: str) -> int:
        """发起连接"""
        self.inited = True
        self.account_id = accountid
        self.path = path
        self.account_type = account_type

        # 创建客户端和账号实例
        session: int = int(float(datetime.now().strftime("%H%M%S.%f")) * 1000)

        self.xt_client = XtQuantTrader(self.path, session)

        self.xt_account = StockAccount(self.account_id, account_type=self.account_type)

        # 注册回调接口
        self.xt_client.register_callback(self)

        # 启动交易线程
        self.xt_client.start()

        # 建立交易连接，返回0表示连接成功
        connect_result: int = self.xt_client.connect()
        if connect_result:
            logger.warning("交易接口连接失败")
            return connect_result

        self.connected = True
        logger.info("交易接口连接成功")

        # 订阅交易回调推送
        subscribe_result: int = self.xt_client.subscribe(self.xt_account)
        if subscribe_result:
            logger.warning("交易推送订阅失败")
            return -1

        logger.info("交易推送订阅成功")

        # 初始化数据查询
        self.query_account()
        self.query_position()
        self.query_order()
        self.query_trade()

        return connect_result

    def new_orderid(self) -> str:
        """生成本地委托号"""
        prefix: str = datetime.now().strftime("1%m%d%H%M%S")

        self.order_count += 1
        suffix: str = str(self.order_count).rjust(6, "0")

        orderid: str = prefix + suffix
        return orderid

    def send_order(self, req: OrderRequest) -> str:
        """委托下单"""
        contract: ContractData = symbol_contract_map.get(req.vt_symbol, None)
        if not contract:
            logger.warning(f"找不到该合约{req.vt_symbol}")
            return ""

        if contract.exchange not in {Exchange.SSE, Exchange.SZSE, Exchange.BSE}:
            logger.warning(f"不支持的合约{req.vt_symbol}")
            return

        if req.type not in {OrderType.LIMIT}:
            logger.warning(f"不支持的委托类型: {req.type.value}")
            return ""

        if req.offset.value:
            if contract.product != Product.OPTION:
                logger.warning("委托失败，现货交易不需要选择开平方向")
                return ""
        else:
            if contract.product == Product.OPTION:
                logger.warning("委托失败，期权交易需要选择开平方向")
                return ""

        stock_code: str = req.symbol + "." + EXCHANGE_VT2XT[req.exchange]
        if self.account_type == "STOCK_OPTION":
            stock_code += "O"

        orderid: str = self.new_orderid()

        self.xt_client.order_stock_async(
            account=self.xt_account,
            stock_code=stock_code,
            order_type=DIRECTION_VT2XT[(req.direction, req.offset)],
            order_volume=int(req.volume),
            price_type=ORDERTYPE_VT2XT[(req.exchange, req.type)],
            price=req.price,
            strategy_name=req.reference,
            order_remark=orderid
        )

        order: OrderData = req.create_order_data(orderid, self.gateway_name)
        # self.gateway.on_order(order)

        return order.vt_orderid

    def cancel_order(self, req: CancelRequest) -> None:
        """委托撤单"""
        sysid: str = self.active_localid_sysid_map.get(req.orderid, None)
        if not sysid:
            logger.warning("撤单失败，找不到委托号")
            return

        if req.exchange == Exchange.SSE:
            market: int = 0
        else:
            market: int = 1

        self.xt_client.cancel_order_stock_sysid_async(self.xt_account, market, sysid)

    def query_position(self) -> None:
        """查询持仓"""
        if self.connected:
            self.xt_client.query_stock_positions_async(self.xt_account, self.on_query_positions_async)

    def query_account(self) -> None:
        """查询账户资金"""
        if self.connected:
            self.xt_client.query_stock_asset_async(self.xt_account, self.on_query_asset_async)

    def query_order(self) -> None:
        """查询委托信息"""
        if self.connected:
            self.xt_client.query_stock_orders_async(self.xt_account, self.on_query_order_async)

    def query_trade(self) -> None:
        """查询成交信息"""
        if self.connected:
            self.xt_client.query_stock_trades_async(self.xt_account, self.on_query_trades_async)

    def close(self) -> None:
        """关闭连接"""
        if self.inited:
            self.xt_client.stop()





