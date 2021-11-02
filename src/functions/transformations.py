import logging
import datetime
import boto3
import json
import os
import re
import copy

import awswrangler as wr
import pandas as pd
import numpy as np

from sqlalchemy import create_engine

from config import settings
from botocore.client import ClientError
from typing import Set

pd.set_option('use_inf_as_na', True)


def process_orders_slot_time(df_orders):
    """
    Parameters
    ----------
    dataframe : orders dataframe with created_datetime info.

    Returns
    -------
    dict.
    """
    # Logging
    logger = logging.getLogger(__name__)
    
    try:
        logger.info("Trying to process Orders SlotTime")
        df = df_orders.copy()

        # === SlotTime Class Definition
        slotTime = SlotTime()

        df = slotTime.set_slot_time(df, 'created_datetime', 'hour')
        df = slotTime.set_week_weekend_features(df, 'created_datetime', 'hour')

        logger.info("Orders SlotTime Successfully created")
        return { "StatusCode": 200, "df": df }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to process Orders Slot Time: {err}" }


def process_orders_reviews_products(df):
    """
    Based on orders by date, slottime, country, branch, brand, products.. get aggregations.
    
    Aggregations:
        reviews - orders with score
        score_1, ..., score_5 - orders with score from 1 to 5
    
    Parameters
    ----------
    df : dataframe with reviews by products

    Returns
    -------
    dict.
    """
    
    try:
        df_ = df.copy()

        # Get Date based on created_datetime order
        df_['date'] = pd.to_datetime(df['created_datetime']).dt.date
        # Subset of interested columns
        df_ = df_[['order_id', 'branch_id', 'branch_name', 'brand_id', 'brand_name', 'date', 'country', 
                   'product_name', 'score_1', 'score_2', 'score_3', 'score_4', 'score_5',
                   'slot_time', 'weekday', 'day_name', 'is_weekend', 'is_week', 'slot_weektime']]
        # For Detail we have duplicated orders because of product
        df_ = df_.drop_duplicates()

        # List of metrics, scores and orders features
        lmetrics = ["order_id", "score_1", "score_2", "score_3", "score_4", "score_5"]
        lgroups_scores = list(set(df_.columns) - set(lmetrics))

        # Generate Score aggregation features
        df_scores_ = df_.groupby(lgroups_scores, observed=True).agg(
                        score_1=('score_1', 'sum'),
                        score_2=('score_2', 'sum'),
                        score_3=('score_3', 'sum'),
                        score_4=('score_4', 'sum'),
                        score_5=('score_5', 'sum')
                    ).reset_index()
        
        # Get "n_reviews"
        df_scores_['n_reviews'] = df_scores_[list(df_scores_.filter(regex='score'))].sum(axis=1).astype(int)
        # Convert to date
        df_scores_['date'] = pd.to_datetime(df_scores_['date'])
        # Get Days from min and max day sale product
        df_scores_["days_product"] = df_scores_.groupby("product_name")['date'].transform(lambda x: (x.max()-x.min()).days)
    
        # Generate Reviews and Days in product to get coefficient
        df_product_coeff = df_scores_.groupby("product_name").agg(
            # Get sum of scores
            n_reviews=('n_reviews', 'sum'),
            days_product=('days_product', 'first')
        ).reset_index()
        df_product_coeff['coeff'] = df_product_coeff['days_product'] / df_product_coeff['n_reviews']
        
        # Cutoff based on distribution:
        # 10%: 1.04, 20%: 2.21, 30%: 4.24, 40%: 7.18, 50%: 10.18
        df_products_ = df_product_coeff[df_product_coeff['coeff'] >= 4]
        df_products_ = df_products_[df_products_['n_reviews'] > 4]
        
        df_scores_ = df_scores_[df_scores_['product_name'].isin(list(df_products_['product_name']))]
        del df_scores_["days_product"]
        
        return { "StatusCode": 200, "df": df_scores_ }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to process Orders with Reviews by Products {err}" }


def process_orders_reviews_errors(df):
    """
    Based on orders by date, slottime, country, branch, brand, score_option.. get aggregations.
    
    score_option could be ERRORS or SUCCESSES
    
    Aggregations:
        reviews - orders with score
        score_1, ..., score_5 - orders with score from 1 to 5
    
    Parameters
    ----------
    df : dataframe with ERROR TYPE or SUCCESSES reviews

    Returns
    -------
    dict.
    """
    
    try:
        df_ = df.copy()

        # Get Date based on created_datetime order
        df_['date'] = pd.to_datetime(df['created_datetime']).dt.date
        # Subset of interested columns
        df_ = df_[['order_id', 'branch_id', 'branch_name', 'brand_id', 'brand_name', 'date', 'country', 
                   'score_option', 'score_1', 'score_2', 'score_3', 'score_4', 'score_5',
                   'slot_time', 'weekday', 'day_name', 'is_weekend', 'is_week', 'slot_weektime']]
        # For Detail we have duplicated orders because of product
        df_ = df_.drop_duplicates()

        # List of metrics, scores and orders features
        lmetrics = ["order_id", "score_1", "score_2", "score_3", "score_4", "score_5"]
        lgroups_scores = list(set(df_.columns) - set(lmetrics))

        # Generate Score aggregation features
        df_scores_ = df_.groupby(lgroups_scores, observed=True).agg(
                        # Get sum of scores
                        n_reviews=('order_id', 'count'),
                        score_1=('score_1', 'sum'),
                        score_2=('score_2', 'sum'),
                        score_3=('score_3', 'sum'),
                        score_4=('score_4', 'sum'),
                        score_5=('score_5', 'sum')
                    ).reset_index()
        
        return { "StatusCode": 200, "df": df_scores_ }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to process Orders with ERROR Types Reviews {err}" }


def process_orders_reviews(df):
    """
    Based on orders by date, slottime, country, branch, brand.. get aggregations.
    Aggregations:
        n_orders - orders made 
        n_orders_score - orders with score
        score_1, ..., score_5 - orders with score from 1 to 5
    
    Parameters
    ----------
    df : dataframe with orders and reviews

    Returns
    -------
    dict.
    """
    
    try:
        df_ = df.copy()

        # Get Date based on created_datetime order
        df_['date'] = pd.to_datetime(df['created_datetime']).dt.date
        # Subset of interested columns
        # rating_type es despreciable.. solo el 1% corresponde a SUPPORT_RATING_CLIENT_FEEDBACK
        #  y estos se encuentran también dentro de RATE_AND_REVIEW_STARS
        df_ = df_[['order_id', 'branch_id', 'branch_name', 'brand_id', 'brand_name', 'date', 'country', 
                   'score_1', 'score_2', 'score_3', 'score_4', 'score_5',
                   'slot_time', 'weekday', 'day_name', 'is_weekend', 'is_week', 'slot_weektime']]
        # For Detail we have duplicated orders because of product
        df_ = df_.drop_duplicates()

        # List of metrics, scores and orders features
        lmetrics = ["order_id", "score_1", "score_2", "score_3", "score_4", "score_5"]
        lgroups_scores = list(set(df_.columns) - set(lmetrics))
        lgroups_orders = ["branch_id", "branch_name", "brand_id", "brand_name", "date", "country",
                          "weekday", "day_name", "is_weekend", "is_week", "slot_time", "slot_weektime"]

        # Generate Score aggregation features
        df_scores_ = df_.groupby(lgroups_scores, observed=True).agg(
                        # Get sum of scores
                        score_1=('score_1', 'sum'),
                        score_2=('score_2', 'sum'),
                        score_3=('score_3', 'sum'),
                        score_4=('score_4', 'sum'),
                        score_5=('score_5', 'sum')
                    ).reset_index()

        # Generate Orders aggregation features
        df_orders_ = df_.groupby(lgroups_orders, observed=True).agg(
                        # Get sum of order_id
                        n_orders=('order_id', 'count')
                    ).reset_index()

        # Merge and return data
        df_orders_scored = pd.merge(df_orders_, df_scores_, how="left", on=lgroups_orders)
        
        return { "StatusCode": 200, "df": df_orders_scored }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to process Orders with Scores {err}" }


def save_postgre(df, table_name, lnumerics, ldates, fg_history):
    """
        Function to Store info in Postgres DB
    """
    logger = logging.getLogger(__name__)

    logger.info("Trying to save in Postgres")
    engine = create_engine(f'postgresql://{settings.PG_USERNAME}:{settings.PG_PASSWORD}@{settings.PG_HOST}:{settings.PG_PORT}/{settings.PG_DATABASE}')

    # CAST
    df[lnumerics] = df[lnumerics].apply(pd.to_numeric)
    df[ldates] = df[ldates].apply(pd.to_datetime)

    if fg_history:
        try:
            # Connect to Database adHoc
            conn = engine.connect()
            df.to_sql(table_name, engine, if_exists='replace', index=False, method="multi")
            return { "StatusCode": 200, "message": "OK" }
        except Exception as err:
            return { "StatusCode": 500, "message": f"Failed while trying to Save Postgres {err}" }
    else:
        try:
            # Connect to Database adHoc
            conn = engine.connect()
            
            # DELETE for update
            #conn.execute(f"DELETE FROM {table_name} WHERE date >= {str()}")
            conn.execute("DELETE FROM {0} WHERE date >= '{1}'".format(table_name, df.date.min().strftime('%Y-%m-%d')))

            df.to_sql(table_name, engine, if_exists='append', index=False, method="multi")
            return { "StatusCode": 200, "message": "OK" }
        except Exception as err:
            return { "StatusCode": 500, "message": f"Failed while trying to Save Postgres {err}" }


class SlotTime(object):
    """
    Class for Create Slot Time and Week/Weekend Flags


    returns Week/end info and Slot Times features
    """
    def __init__(self):
        self.var = {}
        self.early = settings.EARLY
        self.breakfast = settings.BREAKFAST
        self.lunch = settings.LUNCH
        self.afternoon = settings.AFTERNOON
        
    
    def set_slot_time(self, df, dt_col, dt_col_name:str='hour'):
        """
            Apply Slot Time definition based on rules

            return:
                df: dataframe
        """
        df[dt_col_name] = pd.to_datetime(df[dt_col]).dt.hour

        for index, row in df.iterrows():
            if row[dt_col_name] < self.early:
                df.at[index, 'slot_time'] = '01 - Early'
            if self.early <= row[dt_col_name] < self.breakfast:
                df.at[index, 'slot_time'] = '02 - Breakfast'
            if self.breakfast <= row[dt_col_name] < self.lunch:
                df.at[index, 'slot_time'] = '03 - Lunch'
            if self.lunch <= row[dt_col_name] < self.afternoon:
                df.at[index, 'slot_time'] = '04 - Afternoon'
            if row[dt_col_name] >= self.afternoon:
                df.at[index, 'slot_time'] = '05 - Dinner'

        return df
    
    
    def set_week_weekend_features(self, df, dt_col, dt_col_name:str='hour'):
        """
            Create Week/end features

            return:
                df: dataframe
        """
        df["weekday"] = pd.to_datetime(df[dt_col]).dt.weekday
        df["day_name"] = pd.to_datetime(df[dt_col]).dt.day_name()
        # Weekend since Friday 19hs (afternoon)
        df["is_weekend"] = df.apply(lambda r: (r.weekday >= 5) | ((r.weekday == 4) & (r[dt_col_name] >= self.afternoon)), axis=1)
        df["is_week"] = ~df["is_weekend"]
        df["slot_weektime"] = df.apply(lambda r: ("week_" if r.is_week else "weekend_") + r.slot_time, axis=1)

        return df