import pandas as pd
import time
import os
import datetime
import ccxt

pd.set_option('expand_frame_repr', False)  #

TIMEOUT = 6  # 6 second


def crawl_exchanges_datas(exchange_name, symbol, start_time, end_time):
    """
    爬取交易所数据的方法.
    :param exchange_name:  交易所名称.
    :param symbol: 请求的symbol: like BTC/USDT, ETH/USD等。
    :param start_time: like 2018-1-1
    :param end_time: like 2019-1-1
    :return:
    """

    exchange_class = getattr(ccxt, exchange_name)  # 获取交易所的名称 ccxt.binance
    exchange = exchange_class()  # 交易所的类. 类似 ccxt.bitfinex()
    print(exchange)

    current_path = os.getcwd()
    file_dir = os.path.join(current_path, exchange_name, symbol.replace('/', ''))

    if not os.path.exists(file_dir):
        os.makedirs(file_dir)


    start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d')
    end_time = datetime.datetime.strptime(end_time, '%Y-%m-%d')

    start_time_stamp = int(time.mktime(start_time.timetuple())) * 1000
    end_time_stamp = int(time.mktime(end_time.timetuple())) * 1000

    print('开始时间:', start_time_stamp)  # 1529233920000
    print('结束时间:', end_time_stamp)

    limit_count = 100
    if exchange_name == 'bitfinex' or 'bitfinex':
        limit_count = 5000
    elif exchange_name == 'huobipro':
        limit_count = 2000

    while True:
        try:
            print(start_time_stamp)
            data = exchange.fetch_ohlcv(symbol, timeframe='1m', since=start_time_stamp, limit=limit_count)
            df = pd.DataFrame(data)
            if exchange_name == 'huobipro' or exchange_name == 'binance' or exchange_name == 'bittrex' or exchange_name == 'poloniex':
                df.rename(columns={0: 'open_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'basevolume',
                                   6: 'quotevolume'},
                          inplace=True)
            else:
                df.rename(columns={0: 'open_time', 1: 'open', 2: 'high', 3: 'low', 4: 'close', 5: 'basevolume'},
                          inplace=True)

            start_time_stamp = int(df.iloc[-1]['open_time'])  # 获取下一个次请求的时间.

            filename = str(start_time_stamp) + '.csv'
            save_file_path = os.path.join(file_dir, filename)

            print("文件保存路径为：%s" % save_file_path)
            df.set_index('open_time', drop=True, inplace=True)
            df.to_csv(save_file_path)

            if start_time_stamp > end_time_stamp:
                print("完成数据的请求.")
                break

            time.sleep(3)

        except Exception as error:
            print(error)
            time.sleep(10)


def sample_datas(exchange_name, symbol):
    """
    :param exchange_name:
    :param symbol:
    :return:
    """
    path = os.path.join(os.getcwd(), exchange_name, symbol.replace('/', ''))
    print(path)

    file_paths = []
    for root, dirs, files in os.walk(path):
        if files:
            for file in files:
                if file.endswith('.csv'):
                    file_paths.append(os.path.join(path, file))

    file_paths = sorted(file_paths)
    all_df = pd.DataFrame()

    for file in file_paths:
        df = pd.read_csv(file)
        all_df = all_df.append(df, ignore_index=True)

    all_df = all_df.sort_values(by='open_time', ascending=True)

    return all_df


def clear_datas(exchange_name, symbol):
    df = sample_datas(exchange_name, symbol)
    df['open_time'] = df['open_time'].apply(lambda x: (x // 60) * 60)
    print(df)
    df['Datetime'] = pd.to_datetime(df['open_time'], unit='ms') + pd.Timedelta(hours=8)
    df.drop_duplicates(subset=['open_time'], inplace=True)
    df.set_index('Datetime', inplace=True)
    print("*" * 20)
    print(df)
    symbol_path = symbol.replace('/', '')
    df.to_csv(f'{exchange_name}_{symbol_path}_1min_data.csv')


if __name__ == '__main__':
    # crawl_exchanges_datas('coinbasepro', 'BTC/USD', '2018-1-1', '2019-7-22') #limit=300
    # clear_datas('coinbase', 'BTC/USD')

    # crawl_exchanges_datas('binance', 'BTC/USDT', '2019-1-1', '2020-1-1') #yes
    # clear_datas('binance', 'BTC/USDT')

    # crawl_exchanges_datas('okex', 'BTC/USDT', '2019-1-1', '2019-3-1')  # limit=200
    # clear_datas('okex', 'BTC/USDT')

    crawl_exchanges_datas('bitfinex', 'BTC/USDT', '2020-4-1', '2020-4-17')  # limit=110000
    clear_datas('bitfinex', 'BTC/USDT')

    # crawl_exchanges_datas('bitstamp', 'BTC/USD', '2019-1-1', '2020-1-1')  # limit=57
    # clear_datas('bitstamp', 'BTC/USD')

    # crawl_exchanges_datas('bittrex', 'BTC/USDT', '2019-1-1', '2020-1-1')  #limit=5000
    # clear_datas('bittrex', 'BTC/USDT')

    # crawl_exchanges_datas('bitflyer', 'BTC/USD', '2019-1-1', '2020-1-1')  #limit=40
    # clear_datas('bitflyer', 'BTC/USD')

    # crawl_exchanges_datas('poloniex', 'BTC/USDT', '2019-1-1', '2020-1-1')  # limit=110000,timeframe=5m
    # clear_datas('poloniex', 'BTC/USDT')


    # crawl_exchanges_datas('gateio', 'BTC/USDT', '2019-1-1', '2020-1-1')  # limit=1000
    # clear_datas('gateio', 'BTC/USDT')

    # crawl_exchanges_datas('huobipro', 'BTC/USDT', '2019-1-1', '2020-1-1')  # limit=1000
    # clear_datas('huobipro', 'BTC/USDT')
    pass
