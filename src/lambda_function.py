import logging
import datetime
import copy
import boto3
import json

import awswrangler as wr
import pandas as pd
import numpy as np

from config import settings

from functions.loadings import \
    get_rappi_orders_reviews, \
    get_order_product_detail
from functions.transformations import \
    process_orders_slot_time, \
    process_orders_reviews, \
    process_orders_reviews_errors, \
    process_orders_reviews_products, \
    save_postgre
from functions.ingestion import update_quicksight_datasets


pd.options.mode.chained_assignment = None 

# Create a custom logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Handler
c_handler = logging.StreamHandler()
# Format & add it to handler
c_format = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
c_handler.setFormatter(c_format)
# Add handler to logger
logger.addHandler(c_handler)


def lambda_handler(event, context):
    """
    Función lambda
    
    L3 process to store Operations KPIs.
    Info is saved on Postgre.

    Args:
        event: Dictionary with events data:
        {
            fg_history: boolean|integer
            country: country
            app: app ["rappi", "peya"]
        }

        Tables in l3 (postgres): 
        - ops means Operations
            Reviews por Momento del día (fct_ops_***)
                fct_ops_orders_score_slottime
            Reviews con Apertura (Producto/Key Points) (fct_ops_***)
                fct_ops_orders_score_slottime_options
                fct_ops_orders_score_slottime_products
        
        context: None

    Returns:
        dict: Dictionary with runtime data
        {
            statusCode: *200|500*
            body: *ERROR message or Description*
        }
    """

    try:
        # input params
        logger.info(f"Event {event}")

        # Params Definition
        fg_history = event.get("fg_history")
        app = event.get("app", "RP").upper() 
        country = event.get("country", "AR").upper() 
        
        if not(isinstance(fg_history, int)):
            fg_history = 0

        logger.info(f"Input params - country: {country} - app: {app} - HISTORY: {bool(fg_history)}")
        
        # Call to get reviews
        dReviewsResponse = get_rappi_orders_reviews(country, fg_history)
        logger.info(dReviewsResponse["message"])
        # Call to get orders detail
        dOrdersResponse = get_order_product_detail(country, app, fg_history)
        logger.info(dOrdersResponse["message"])

        # StatusCode Definition
        status_code = 200 if ((dReviewsResponse['StatusCode']==200) & \
                              (dOrdersResponse['StatusCode']==200)) else 500

        if status_code == 200:
            df = pd.merge(dOrdersResponse['df'], dReviewsResponse['df'], how="left", on="order_id")
            # Process Slottime
            dresponse = process_orders_slot_time(df)

            if dresponse['StatusCode'] == 200:
                df = dresponse['df']

                ######################################
                # Orders and Reviews Slottime: GRAL
                dresponse = process_orders_reviews(df)
                df_orders_score_slottime = dresponse['df']
                df_orders_score_slottime['score_n_orders']= df_orders_score_slottime[list(df_orders_score_slottime.filter(regex='score'))].sum(axis=1).astype(int)
                df_orders_score_slottime['app'] = app

                # List of Numeric Columns
                lnumerics = ["branch_id", "brand_id", "weekday", "n_orders", "score_n_orders", 
                            "score_1", "score_2", "score_3", "score_4", "score_5"]
                # List of Date Columns
                ldates = ["date"]

                dresponse = save_postgre(df_orders_score_slottime, 
                                        'fct_ops_orders_score_slottime', 
                                        lnumerics, 
                                        ldates, 
                                        fg_history)
                logger.info(dresponse['message'])

                ######################################
                # Reviews with ERROR TYPE Definition
                dresponse = process_orders_reviews_errors(df)
                df_orders_score_slottime_options = dresponse['df']
                df_orders_score_slottime_options['app'] = app

                # List of Numeric Columns
                lnumerics = ["branch_id", "brand_id", "weekday", "n_reviews", 
                            "score_1", "score_2", "score_3", "score_4", "score_5"]
                # List of Date Columns
                ldates = ["date"]

                dresponse = save_postgre(df_orders_score_slottime_options, 
                                        'fct_ops_orders_score_slottime_options', 
                                        lnumerics, 
                                        ldates, 
                                        fg_history)
                logger.info(dresponse['message'])

                #######################################
                # Reviews with PRODUCTs
                dresponse = process_orders_reviews_products(df)
                df_orders_score_slottime_products = dresponse['df']
                df_orders_score_slottime_products['app'] = app

                # List of Numeric Columns
                lnumerics = ["branch_id", "brand_id", "weekday", "n_reviews", 
                            "score_1", "score_2", "score_3", "score_4", "score_5"]
                # List of Date Columns
                ldates = ["date"]

                dresponse = save_postgre(df_orders_score_slottime_products, 
                                        'fct_ops_orders_score_slottime_products', 
                                        lnumerics, 
                                        ldates, 
                                        fg_history)
                logger.info(dresponse['message'])


                # Update ops datasets
                input_ingestion = { 'datasets': ['fct_ops_'] }
                response = update_quicksight_datasets(input_ingestion)
                if response["StatusCode"] == 200:
                    logger.info(response["message"])
                    return { "StatusCode": 200, "message": "OK"}
                else:
                    return { "StatusCode": 500, "message": f"{ response['message'] }" }


            else:
                return { "StatusCode": 500, "message": dresponse['message'] }
        else:
            return { "StatusCode": 500, "message": "ERROR while trying to get Reviews or Orders" }
    except BaseException as err:
        logging.critical("Exception raised: %s", str(err), exc_info=True)
        return {"statusCode": 500, "body": "ERROR"}