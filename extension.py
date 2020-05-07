#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import sys
import numpy as np
import pandas as pd
import datetime as dt
#
from logbook import Logger
#
from zipline.utils.cli import maybe_show_progress

log = Logger(__name__)

from zipline.data.bundles import register
#from zipline.data.bundles.yahoo_csv import yahoo_csv_equities # yahoo_csv.py need to be placed in zipline.data.bundles
from zipline.data.bundles.csvdir import csvdir_equities

tickers = {
     "WLD.PA", "CRB.PA","CBU7.AS","IDTL.L", "IAUP.L" #"IDTL.L", "CRB.PA", "IAUP.L", "CBU7.AS"
}

tickers_us = {
     "SPY", "GLD","DBC","IEF", "TLT" #"IDTL.L", "CRB.PA", "IAUP.L", "CBU7.AS"
}

tickers_as = {
     "CBU7.AS",#"IDTL.L", "CRB.PA", "IAUP.L", "CBU7.AS"
}

tickers_l = {
     "IDTL.L", #"IAUP.L",#"IDTL.L", "CRB.PA", "IAUP.L", "CBU7.AS"
}
def get_path(path=None):
    if path is None:
        path = os.environ.get('YAHOO_CSV_BUNDLE')
    if path is None:
        log.error("environment variable 'YAHOO_CSV_BUNDLE' is not set")
        sys.exit(1)
    if not os.path.isdir(path):
        log.error("{0} is not a directory".format(path))
        sys.exit(1)
    return path


def find_csv_files(path=None):
    symbols = sorted(item.split('.csv')[0]
                     for item in os.listdir(path) if item.endswith('.csv'))
    return symbols


def yahoo_csv_equities(ticker_symbols, path=None, start=None, end=None, exchange='YAHOO'):

    # ingest function
    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               start=start,
               end=end):

        def reader(symbols):
            with maybe_show_progress(symbols, show_progress, label='Loading csv files: ') as it:
                for sid, symbol in enumerate(it):
                    file_path = '{0}/{1}.csv'.format(path, symbol)
                    if os.path.exists(file_path):
                        df_data = pd.read_csv(file_path, index_col='Date', parse_dates=True).sort_index()
                        df_data.rename(
                            columns={
                                'Open': 'open',
                                'High': 'high',
                                'Low': 'low',
                                'Close': 'close',
                                'Volume': 'volume',
                                #'Adj Close': 'price',
                            },
                            inplace=True,
                        )
                        #df_data['volume'] = df_data['volume']/1000
                        start_date = df_data.index[0]
                        end_date = df_data.index[-1]
                        # The auto_close date is the day after the last trade.
                        autoclose_date = end_date + pd.Timedelta(days=1)
                        df_metadata.iloc[sid] = start_date, end_date, autoclose_date, symbol
                        yield sid, df_data
        #
        csv_path = get_path(path)
        symbols = tuple(find_csv_files(csv_path) if len(ticker_symbols) == 0 else ticker_symbols)
        log.info('target symbols are: {0}'.format(symbols))
        df_metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),]))
        log.info('writing data...'.format())
        daily_bar_writer.write(reader(symbols), show_progress=show_progress)
        df_metadata['exchange'] = exchange
        log.info('meta data: {0}'.format(df_metadata))
        # symbol_map = pd.Series(df_metadata.symbol.index, df_metadata.symbol)
        # log.info('symbol map generated: {0}'.format(symbol_map))
        asset_db_writer.write(equities=df_metadata)
        adjustment_writer.write()
        log.info('writing completed'.format())
    #
    return ingest


register(
    'yahoo_csv_bundle1',
    yahoo_csv_equities({"AAPL"}, path='~/csv/'),
)
register(
    'yahoo_csv_bundle2',
    yahoo_csv_equities({}, path='~/csv/'),
)

register(
    'yahoo_csv_bundle_allweather',
    yahoo_csv_equities(tickers, path='/Users/cedriss/csv/'),
    calendar_name = "XPAR",
)

register(
    'yahoo_csv_bundle_allweather_us',
    yahoo_csv_equities(tickers_us, path='/Users/cedriss/csv/'),
    calendar_name = "NYSE",
)

register(
    'yahoo_csv_bundle_allweather_as',
    yahoo_csv_equities(tickers_as, path='/Users/cedriss/csv/'),
    calendar_name = "XPAR",
)

register(
    'yahoo_csv_bundle_allweather_l',
    yahoo_csv_equities(tickers_l, path='/Users/cedriss/csv/'),
    calendar_name = "XPAR",
)

register(
    'AWP-bundle',  # name we select for the bundle
    csvdir_equities(
        # name of the directory as specified above (named after data frequency)
        ['Daily'],
        # path to directory containing the
        '/Users/cedriss/csv/',
    ),
    calendar_name='NYSE',  # NYSE

)










