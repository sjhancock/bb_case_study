#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from distributed import Client, as_completed
import dask.dataframe as dd
import pandas as pd
import logging
from datetime import datetime
from os.path import join
import time
from pathlib import Path
import json
import warnings

# Suppress FutureWarning messages from pandas concat function
#
warnings.simplefilter(action='ignore', category=FutureWarning)


def init_logging(name: str):
    """
    function to a setup the logger to create a log file <name>YYYY-M-D_H:M:S.logs in the logs directory
    :param name: the name of the log file
    :return: the logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    c_handler = logging.StreamHandler()
    c_handler.setLevel(logging.DEBUG)
    c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    logger.addHandler(c_handler)

    Path("logs").mkdir(parents=True, exist_ok=True)
    f_handler = logging.FileHandler(join('logs', f"{name}_{datetime.utcnow().strftime('%Y-%m-%d_%H:%M:%S')}.logs"))
    f_handler.setLevel(logging.DEBUG)
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_handler.setFormatter(f_format)
    logger.addHandler(f_handler)
    return logger


def get_executions(logger) -> pd.DataFrame:
    """
    function to read in the data/executions.parquet file and log the count the number of executions, venues and trade dates
    :param logger: the logger to write into. Throws a ValueError if fields are missing in the parquet file
    :return: a pandas dataframe of executions
    """

    logger.debug('Starting Task 1')

    executions_df = pd.read_parquet(join('..', 'data', 'executions.parquet'))

    logger.debug(f"     # executions {len(executions_df)}")

    if 'Venue' in executions_df.columns:
        logger.debug(f"     # venues {executions_df['Venue'].nunique()}")
    else:
        error = 'get_execution: Venue column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    if 'TradeTime' in executions_df.columns:

        # will need the date of the trades so adjust format
        #
        executions_df['TradeTime'] = executions_df['TradeTime'].astype('datetime64[ns]')
        executions_df['TradeDate'] = executions_df['TradeTime'].dt.date

        logger.debug(f"     # venue trade date counts:")

        stats = executions_df.groupby(['Venue', 'TradeDate']).size().to_dict()
        for stat in stats:
            logger.debug(f"         {stat[0]}, {stat[1]}:  {stats[stat]}")

    else:
        error = 'get_execution: TradeTime column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    logger.debug('Task 1 complete')

    return executions_df


def data_cleansing(logger, executions_df: pd.DataFrame) -> pd.DataFrame:
    """
    a function to clean the executions dataframe of all records except for CONTINUOUS_TRADING Phases
    :param logger: the logger to write into
    :param executions_df: the executions dataframe
    :return: a filtered executions dataframe
    """
    logger.debug('Starting Task 2')
    if 'Phase' in executions_df.columns:
        executions_df = executions_df.loc[executions_df['Phase'] == 'CONTINUOUS_TRADING']
        logger.debug(f"     Filtering on CONTINUOUS_TRADING # executions {len(executions_df)}")
    else:
        error = 'data_cleansing: Phase column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    logger.debug('Task 2 complete')

    return executions_df


def data_transformation(logger, executions_df: pd.DataFrame) -> pd.DataFrame:
    """
    a function to enrich the executions dataframe with reference data from the data/refdata.parquet file
    :param logger: the logger
    :param executions_df: the executions dataframe to enrich
    :return: the enriched executions dataframe
    """
    logger.debug('Starting Task 3')

    if 'Quantity' in executions_df.columns:
        executions_df['Side'] = executions_df['Quantity'].apply(lambda x: 1 if x >= 0 else 2)

        # Check count of Side transactions
        side_1_df = executions_df.loc[executions_df['Side'] == 1]
        logger.debug(f"     Side == 1 # executions {len(side_1_df)}")
        side_2_df = executions_df.loc[executions_df['Side'] == 2]
        logger.debug(f"     Side == 1 # executions {len(side_2_df)}")

    else:
        error = 'data_transformation: Quantity column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    if 'ISIN' not in executions_df.columns:
        error = 'data_transformation: ISIN column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    reference_df = pd.read_parquet(join('..', 'data', 'refdata.parquet'))

    # these are the fields we want from refdata
    #
    columns_req = ['ISIN', 'primary_ticker', 'primary_mic', 'id']
    for col in columns_req:
        if col not in reference_df.columns:
            error = f'data_transformation: {col} column missing in reference_df'
            logger.error(error)
            raise ValueError(error)

    # note it's an inner join on the ISIN fields
    #
    executions_df = executions_df.merge(reference_df[columns_req], how='inner', on='ISIN')

    logger.debug(f"     check columns added {list(executions_df.columns)}")

    logger.debug('Task 3 complete')

    return executions_df


def tca_enrich(security_id, executions_df):
    """
    a function (designed to be parallelised by Dask) to enrich a specific security's executions  with best bids, asks, mids and slippage
    :param security_id: the id of the security to enrich
    :param executions_df: the executions dataframe to enrich
    :return: an enriched executions dataframe
    """
    tca_executions = []

    # filter the transactions on the required security
    #
    executions_df = executions_df[executions_df['id'] == security_id]

    if len(executions_df) > 0:

        # we should filter the large market data file on just the security and CONTINUOUS_TRADING market_state prices
        #
        filters = [('listing_id', '==', int(security_id)),
                   ('market_state', '==', 'CONTINUOUS_TRADING'),
                   ]

        # use Dask to read this in but convert to Pandas dataframe as it should fit in memory
        #
        market_df = dd.read_parquet(join('..', 'data', 'marketdata.parquet'), filters=filters).compute()

        # ensure speedy filtering on event_timestamp
        #
        market_df['event_timestamp'] = market_df['event_timestamp'].astype('datetime64[ns]')
        market_df.set_index('event_timestamp')
        market_df.sort_index()

        # for each transaction
        #  - get the market data stamp nearest to trade datetime - 1sec
        #  - get the market data stamp nearest to trade datetime
        #  - get the market data stamp nearest to trade datetime + 1 sec
        #  - extract the bids, asks and calc the mids
        #  - calc the slippage
        #
        for index, row in executions_df.iterrows():

            minus_1_df = market_df.loc[(market_df['event_timestamp'] <= row['TradeTime'] - pd.Timedelta('1 sec')) & (market_df['primary_mic'] == row['Venue'])].tail(1)

            trade_df = market_df.loc[(market_df['event_timestamp'] <= row['TradeTime']) & (market_df['primary_mic'] == row['Venue'])].tail(1)

            plus_1_df = market_df.loc[(market_df['event_timestamp'] >= row['TradeTime'] + pd.Timedelta('1 sec')) & (market_df['primary_mic'] == row['Venue'])].head(1)

            # we will need the existing fields and values for this transaction in the output dataframe
            #
            tca_row = row.to_dict()

            if len(trade_df) > 0:
                tca_row['best_bid'] = trade_df.iloc[0]['best_bid_price']
                tca_row['best_ask'] = trade_df.iloc[0]['best_ask_price']
                tca_row['mid_price'] = (tca_row['best_bid'] + tca_row['best_ask']) / 2
                if tca_row['Side'] == 1:
                    tca_row['slippage'] = (tca_row['best_ask'] - tca_row['Price']) / (tca_row['best_ask'] - tca_row['best_bid'])
                else:
                    tca_row['slippage'] = (tca_row['Price'] - tca_row['best_bid']) / (tca_row['best_ask'] - tca_row['best_bid'])

            else:
                tca_row['best_bid'] = None
                tca_row['best_ask'] = None
                tca_row['mid_price'] = None
                tca_row['slippage'] = None

            if len(minus_1_df) > 0:
                tca_row['best_bid_min_1s'] = minus_1_df.iloc[0]['best_bid_price']
                tca_row['best_ask_min_1s'] = minus_1_df.iloc[0]['best_ask_price']
                tca_row['mid_price_min_1s'] = (tca_row['best_bid_min_1s'] + tca_row['best_ask_min_1s']) / 2
            else:
                tca_row['best_bid_min_1s'] = None
                tca_row['best_ask_min_1s'] = None
                tca_row['mid_price_min_1s'] = None

            if len(plus_1_df) > 0:
                tca_row['best_bid_1s'] = plus_1_df.iloc[0]['best_bid_price']
                tca_row['best_ask_1s'] = plus_1_df.iloc[0]['best_ask_price']
                tca_row['mid_price_1s'] = (tca_row['best_bid_1s'] + tca_row['best_ask_1s']) / 2
            else:
                tca_row['best_bid_1s'] = None
                tca_row['best_ask_1s'] = None
                tca_row['mid_price_1s'] = None
            tca_executions.append(tca_row)

        # the enriched transaction dataframe
        #
        tca_executions_df = pd.DataFrame(tca_executions)
    else:
        tca_executions_df = None
    return tca_executions_df


def data_calculations(logger, dask_cluster, executions_df: pd.DataFrame) -> pd.DataFrame:
    """
    a function to perform the bid/ask calculations on all security executions using Dask cluster
    :param logger: the logger to write into
    :param dask_cluster: the client connected to a Dask Cluster
    :param executions_df: the executions dataframe
    :return: the enriched executions dataframe
    """
    logger.debug('Starting Task 4')

    results = []
    remote_df = dask_cluster.scatter(executions_df)

    # get the unique list of securities with transaction, submit to the Dask cluster and gather the resulting Futures
    #
    securities = executions_df['id'].unique()
    for idx in range(len(securities)):
        logger.debug(f'     Submitting tca calc for security {securities[idx]}')

        results.append(dask_cluster.submit(tca_enrich, securities[idx], remote_df))

    # Process the Futures that have completed
    #
    tca_enriched_df = None
    for future, result_df in as_completed(results, with_results=True):
        if result_df is not None:
            logger.debug(f"     Received tca calc for security {result_df.iloc[0]['id']}")

            # Union the resulting dataframes
            if tca_enriched_df is None:
                tca_enriched_df = result_df
            else:
                tca_enriched_df = pd.concat([tca_enriched_df, result_df], ignore_index=True)
        else:
            logger.error(f"Received error from cluster")

    # re-sort in date time order because the Futures will complete asynchronously
    #
    tca_enriched_df = tca_enriched_df.sort_values(by=['TradeTime'])
    tca_enriched_df = tca_enriched_df.reset_index()

    logger.debug(f"     Price enriched # executions {len(tca_enriched_df)}")
    null_df = tca_enriched_df.loc[tca_enriched_df['best_bid'].isnull()]
    not_null_df = tca_enriched_df.loc[tca_enriched_df['best_bid'].notnull()]
    logger.debug(f"     Prices missing # executions {len(null_df)}")
    logger.debug(f"     Prices found # executions {len(not_null_df)}")

    logger.debug('Task 4 complete')

    return tca_enriched_df


def analysis(name: str):
    """
    main function to start (and stop) the cluster and then perform all steps in the analysis
    :param name: the name for the log file
    :return: None
    """

    logger = init_logging(name=name)

    # start the dask cluster
    #
    client = Client()

    try:
        logger.info(f"Dask Dashboard link: {client.dashboard_link}")

        start_time = time.time()
        executions_df = get_executions(logger=logger)
        stats = {'get_executions_sec': time.time() - start_time}

        start_time_2 = time.time()
        executions_df = data_cleansing(logger=logger, executions_df=executions_df)
        stats['data_cleansing_sec'] = time.time() - start_time_2

        start_time_3 = time.time()
        executions_df = data_transformation(logger=logger, executions_df=executions_df)
        stats['data_transformation_sec'] = time.time() - start_time_3

        start_time_4 = time.time()
        executions_df = data_calculations(logger=logger, dask_cluster=client, executions_df=executions_df)
        stats['data_calculations_sec'] = time.time() - start_time_4

        start_time_5 = time.time()
        executions_df.to_parquet(join('..', 'data', 'enriched_executions.parquet'))
        end_time = time.time()
        stats['write_output_sec'] = end_time - start_time_5
        stats['total_sec'] = end_time - start_time

        logger.info(f"{json.dumps(stats, indent=4)}")

    except ValueError as e:
        logger.error(f"{e}", exc_info=True)

    # close the cluster down
    #
    client.close()


if __name__ == '__main__':

    analysis('case_study')
