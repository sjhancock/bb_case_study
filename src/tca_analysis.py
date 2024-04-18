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


def init_logging(name: str):
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

    logger.debug('Starting task 1')

    # note - maintained misspelling of file name !
    executions_df = pd.read_parquet(join('..', 'data', 'executions.parquet'))

    logger.debug(f"# executions {len(executions_df)}")

    if 'Venue' in executions_df.columns:
        logger.debug(f"# venues {executions_df['Venue'].nunique()}")
    else:
        error = 'get_execution: Venue column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    if 'TradeTime' in executions_df.columns:
        # will need the date of the trades so adjust format
        #
        executions_df['TradeTime'] = executions_df['TradeTime'].astype('datetime64[ns]')
        executions_df['TradeDate'] = executions_df['TradeTime'].dt.date
        logger.debug(f"# venue trade date counts:")
        stats = executions_df.groupby(['Venue', 'TradeDate']).size().to_dict()
        for stat in stats:
            logger.debug(f"{stat[0]}, {stat[1]}:  {stats[stat]}")
    else:
        error = 'get_execution: TradeTime column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    return executions_df


def data_cleansing(logger, executions_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Starting task 2')
    if 'Phase' in executions_df.columns:
        executions_df = executions_df.loc[executions_df['Phase'] == 'CONTINUOUS_TRADING']
        logger.debug(f"# executions {len(executions_df)}")
    else:
        error = 'data_cleansing: Phase column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    return executions_df


def data_transformation(logger, executions_df: pd.DataFrame) -> pd.DataFrame:
    logger.debug('Starting task 3')

    if 'Quantity' in executions_df.columns:
        executions_df['Side'] = executions_df['Quantity'].apply(lambda x: 1 if x >= 0 else 2)
    else:
        error = 'data_transformation: Quantity column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    if 'ISIN' not in executions_df.columns:
        error = 'data_transformation: ISIN column missing in executions_df'
        logger.error(error)
        raise ValueError(error)

    reference_df = pd.read_parquet(join('..', 'data', 'refdata.parquet'))
    columns_req = ['ISIN', 'primary_ticker', 'primary_mic', 'id']
    for col in columns_req:
        if col not in reference_df.columns:
            error = f'data_transformation: {col} column missing in reference_df'
            logger.error(error)
            raise ValueError(error)

    executions_df = executions_df.merge(reference_df[columns_req], how='inner', on='ISIN')

    return executions_df


def tca_enrich(listing_id, executions_df):
    tca_executions = []
    executions_df = executions_df[executions_df['id'] == listing_id]
    if len(executions_df) > 0:

        filters = [('listing_id', '==', int(listing_id)),
                   ('market_state', '==', 'CONTINUOUS_TRADING'),
                   ]

        market_df = dd.read_parquet(join('..', 'data', 'marketdata.parquet'), filters=filters).compute()

        market_df['event_timestamp'] = market_df['event_timestamp'].astype('datetime64[ns]')
        market_df.set_index('event_timestamp')
        market_df.sort_index()

        for index, row in executions_df.iterrows():

            minus_1_df = market_df.loc[(market_df['event_timestamp'] <= row['TradeTime'] - pd.Timedelta('1 sec')) & (market_df['primary_mic'] == row['Venue'])].tail(1)

            trade_df = market_df.loc[(market_df['event_timestamp'] <= row['TradeTime']) & (market_df['primary_mic'] == row['Venue'])].tail(1)

            plus_1_df = market_df.loc[(market_df['event_timestamp'] >= row['TradeTime'] + pd.Timedelta('1 sec')) & (market_df['primary_mic'] == row['Venue'])].head(1)

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

        tca_executions_df = pd.DataFrame(tca_executions)
    else:
        tca_executions_df = None
    return tca_executions_df


def data_calculations(logger, dask_cluster, executions_df: pd.DataFrame):
    logger.debug('Starting task 4')

    securities = executions_df['id'].unique()
    results = []
    remote_df = dask_cluster.scatter(executions_df)

    for idx in range(len(securities)):
        logger.debug(f'Submitting tca calc for security {securities[idx]}')
        results.append(dask_cluster.submit(tca_enrich, securities[idx], remote_df))

    tca_enriched_df = None
    for future, result_df in as_completed(results, with_results=True):
        if result_df is not None:
            logger.debug(f"Received tca calc for security {result_df.iloc[0]['id']}")

            if tca_enriched_df is None:
                tca_enriched_df = result_df
            else:
                tca_enriched_df = pd.concat([tca_enriched_df, result_df], ignore_index=True)
        else:
            logger.error(f"Received error from cluster")

    tca_enriched_df = tca_enriched_df.sort_values(by=['TradeTime'])
    tca_enriched_df = tca_enriched_df.reset_index()

    return tca_enriched_df


def analysis(name: str):
    """
    main function to perform all steps in the processing
    :param name: the name for the log file
    :return:
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

        logger.info(f"{stats}")

    except ValueError as e:
        logger.error(f"{e}", exc_info=True)

    # close the cluster down
    #
    client.close()


if __name__ == '__main__':

    analysis('case_study')
