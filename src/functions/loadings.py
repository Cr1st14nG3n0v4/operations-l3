import logging
import datetime
import boto3

import awswrangler as wr
import pandas as pd
import numpy as np

from config import settings
from botocore.client import ClientError
from typing import Set


def get_rappi_orders_reviews(country="AR", fg_history=0):
    """
    Get RAPPI Reviews.-

    Args:
        country: ["AR", "CL"], "AR" by default
        fg_history: [0, 1], 0 by default, means that only read actual and previous month data

    Returns:
        dict: Dictionary
            'StatusCode': [200, 500]
            'df': df.dataframe
    """
    logger = logging.getLogger(__name__)

    try:
        path = settings.L2_RAPPI_USERS_S3

        partition_filter = lambda x: x["country"] == country and x["app"] == "RP" 
        if not fg_history:
            # If not history run, previous and actual month
            actualMonth = (datetime.date.today()).strftime("%Y-%m")
            previousMonth = (datetime.date.today().replace(day=1) - datetime.timedelta(days=1)).strftime("%Y-%m")
            logger.info(f"Reading Reviews for { actualMonth } and { previousMonth }")
            partition_filter = lambda x: x["country"] == country and \
                                         x["app"] == "RP" and \
                                        (x["year_month"] == previousMonth or x["year_month"] == actualMonth)
        
        df_reviews = wr.s3.read_parquet(path=path, dataset=True, use_threads=True, partition_filter=partition_filter)
        
        # Subset of interested Columns
        df_reviews = df_reviews[['order_id', 'reviewed_at', 'rating_type', 'score', 'option', 
                                 'score_1', 'score_2', 'score_3', 'score_4', 'score_5']]
        
        # Group option values
        df_reviews['option'] = df_reviews['option'].apply(lambda x: settings.DOPTIONS[x] if x in settings.DOPTIONS else x)
        df_reviews = df_reviews.rename(columns={'option': 'score_option'})
        
        return { "StatusCode": 200, "message": "RAPPI Reviews OK", "df": df_reviews }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to read RAPPI Reviews: {err}" }


def get_order_product_detail(country="AR", app="RP", fg_history=0):
    """
    Parameters
    ----------
    path : S3 definition.

    Returns
    -------
    dict.

    """    
    logger = logging.getLogger(__name__)

    try:
        path = settings.L2_ORDER_DETAIL_POPAPP_S3
        app = 'rappi' if app == 'RP' else 'peya'
        
        partition_filter = lambda x: x["country"] == country and x["app"] == app
        if not fg_history:
            # If not history run, previous and actual month
            lastMonth = datetime.date.today().replace(day=1) - datetime.timedelta(days=1)
            date_from = lastMonth.replace(day=1)
            logger.info(f"Reading Orders since { date_from.strftime('%Y-%m-%d') }")
            partition_filter = lambda x: x["country"] == country and \
                                         x["app"] == app and \
                                         date_from <= datetime.date(int(x["year"]), int(x["month"]), int(x["day"]))
            
        df_orders = wr.s3.read_parquet(path=path, dataset=True, use_threads=True, partition_filter=partition_filter)
        
        # Subset orders with "product" type
        df_orders_products = df_orders[df_orders['type'] == 'product']
        
        df_orders_products['order_id'] = df_orders_products['order_id'].astype(int)
        
        # Subset of interested Columns
        ldrop = ['topping_id', 'topping_name', 'topping_units', 'topping_price']
        df_orders_products.drop(ldrop, axis=1, inplace=True)

        return { "StatusCode": 200, "message": "AWS Orders PopAPP OK", "df": df_orders_products }
    except Exception as err:
        return { "StatusCode": 500, "message": f"Failed while trying to read AWS Orders PopAPP {err}" }