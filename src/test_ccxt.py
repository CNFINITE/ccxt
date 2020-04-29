import ccxt
import pandas as pd

# print(ccxt.exchanges)
#
huobi = ccxt.huobipro()
binance = ccxt.binance()
okex = ccxt.okex()
coinbase = ccxt.coinbasepro()
bitfinex = ccxt.bitfinex()
kraken = ccxt.kraken()
bittrex = ccxt.bittrex()
bitflyer = ccxt.bitflyer()
poloniex = ccxt.poloniex()
coincheck = ccxt.coincheck()
gateio = ccxt.gateio()
bitstamp = ccxt.bitstamp()
# market = huobi.load_markets()  # BTC/USDT
# print(huobi.market)
symbol = 'BTC/USDT'

# 实时行情：获取聚合行情（ticker）数据同时提供最近24小时的交易聚合信息
huobi_btc_ticker = huobi.fetch_ticker('BTC/USDT')
binance_btc_ticker = binance.fetch_ticker('BTC/USDT')
# print(huobi_btc_ticker)
# print(binance_btc_ticker)


# ohlcv数据
ohlcv_huobi = huobi.fetch_ohlcv(symbol, timeframe='15m')
ohlcv_binance = binance.fetch_ohlcv(symbol, timeframe='15m')
ohlcv_okex = okex.fetch_ohlcv(symbol, timeframe='1m')
ohlcv_coinbase = coinbase.fetch_ohlcv('BTC/USD', timeframe='1m')
ohlcv_bitfinex = bitfinex.fetch_ohlcv(symbol, timeframe='15m')
# ohlcv_kraken = kraken.fetch_ohlcv(symbol, timeframe='15m')
ohlcv_bittrex = bittrex.fetch_ohlcv(symbol, timeframe='30m')
ohlcv_bitflyer = bitflyer.fetch_ohlcv('BTC/USD', timeframe='15m')
ohlcv_poloniex = poloniex.fetch_ohlcv(symbol, timeframe='15m')
ohlcv_coincheck = coincheck.fetch_ohlcv('BTC/JPY', timeframe='15m')
ohlcv_gateio = gateio.fetch_ohlcv(symbol, timeframe='15m')

# 深度信息
limit = 10
depth_huobi = huobi.fetch_order_book(symbol, limit)
# print(depth_huobi)

# 获取当前的最好价格（查询市场价格）并且计算买入卖出的价差
orderbook = huobi.fetch_order_book(symbol)
bid = orderbook['bids'][0][0] if len(orderbook['bids']) > 0 else None
ask = orderbook['asks'][0][0] if len(orderbook['asks']) > 0 else None
spread = (ask - bid) if (bid and ask) else None
# print(huobi.id, 'market price', {'bid': bid, 'ask': ask, 'spread': spread})


# 获取指定交易对的最近交易记录
huobi_trade = huobi.fetch_trades(symbol=symbol)
# print(huobi_trade)


pd.set_option('display.max_columns', None)
pd.set_option('display.width', 5000)
pd.set_option('display.float_format', lambda x: '%.3f' % x)
#
# df_huobi = pd.DataFrame(ohlcv_huobi,
#                         columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount",
#                                  "vol"])
# df_huobi['timestamp'] = pd.to_datetime(df_huobi['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_huobi)

# df_binance = pd.DataFrame(ohlcv_binance, columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount", "vol"])
# df_binance['timestamp'] = pd.to_datetime(df_binance['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_binance)

# df_okex = pd.DataFrame(ohlcv_okex,
#                        columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount"])
# df_okex['timestamp'] = pd.to_datetime(df_okex['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_okex)

# df_coinbase = pd.DataFrame(ohlcv_coinbase, columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount"])
# df_coinbase['timestamp'] = pd.to_datetime(df_coinbase['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_coinbase)

df_bitfinex = pd.DataFrame(ohlcv_bitfinex,
                           columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount"])
df_bitfinex['timestamp'] = pd.to_datetime(df_bitfinex['timestamp'], unit='ms') + pd.Timedelta(hours=8)
print(df_bitfinex)

# df_kraken = pd.DataFrame(ohlcv_kraken,
#                          columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "vol"])
# df_kraken['timestamp'] = pd.to_datetime(df_kraken['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_kraken)

# df_bittrex = pd.DataFrame(ohlcv_bittrex,
#                           columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount",
#                                    "volume"])
# df_bittrex['timestamp'] = pd.to_datetime(df_bittrex['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_bittrex)

# df_bitflyer = pd.DataFrame(ohlcv_bitflyer,
#                          columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount"])
# df_bitflyer['timestamp'] = pd.to_datetime(df_bitflyer['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_bitflyer)

# df_poloniex = pd.DataFrame(ohlcv_poloniex,
#                            columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount",
#                                     "vol"])
# df_poloniex['timestamp'] = pd.to_datetime(df_poloniex['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_poloniex)

# df_coincheck = pd.DataFrame(ohlcv_coincheck,
#                             columns=["timestamp", "open_price", "high_price", "low_price", "closing_price", "amount"])
# df_coincheck['timestamp'] = pd.to_datetime(df_coincheck['timestamp'], unit='ms') + pd.Timedelta(hours=8)
# print(df_coincheck)

# 期货
exchange = ccxt.okex()
exchange.load_markets()
for symbol in exchange.markets:
    market = exchange.markets[symbol]
    # print(market.keys())
    if market['futures']:
        print('----------------------------------------------------')
        print(symbol, exchange.fetch_ticker(symbol))
        time.sleep(exchange.rateLimit / 1000)
#
