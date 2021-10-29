import logging
import datetime
import boto3
import os

import awswrangler as wr

from typing import Optional, List, Dict, Any
from config import settings


def list_datasets(account_id: Optional[str] = None, boto3_session: Optional[boto3.Session] = None) -> List[Dict[str, Any]]:
    """
        List all QuickSight datasets summaries.

        Parameters
        ----------
        account_id : str, optional
            If None, the account ID will be inferred from your boto3 session.
        boto3_session : boto3.Session(), optional
            Boto3 Session. The default boto3 session will be used if boto3_session receive None.

        Returns
        -------
        List[Dict[str, Any]]
            Datasets summaries.
    """
    return _list(
        func_name="list_data_sets", attr_name="DataSetSummaries", account_id=account_id, boto3_session=boto3_session
    ) 


def create_ingestion(account_id: Optional[str] = None, ldatasets_ids=[]):
    """
        Creates and starts a new SPICE ingestion on a dataset

        Parameters
        ----------
        account_id : str, optional
            If None, the account ID will be inferred from Quicksight boto3 session.
        ldatasets_ids : list
            Datasets ID's list

        Returns
        -------
        Response message
    """
    # Logging
    logger = logging.getLogger(__name__)
    logger.info("Getting Quicksight Client")
    client = boto3.client('quicksight')
    logger.info("Creating SPICE Ingestion")
    for dataset_id in ldatasets_ids:
        dt_string = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
        ingestion_id = dt_string + "-" + dataset_id
        logger.info(f"Creating ingestion {ingestion_id} for dataset {dataset_id}")
        try:
            response = client.create_ingestion(AwsAccountId=account_id, 
                                                   DataSetId=dataset_id, 
                                                   IngestionId=ingestion_id)
            logger.info(response)
        except Exception as err:
            logger.info(err)
            return { "StatusCode": 500, "message": "ERROR" }
    return { "StatusCode": 200, "message": "OK" }


def update_quicksight_datasets(ddatasets):
    """
        Lambda function for create ingestion in Quicksight's datasets.

        This Lambda Function is called from other Lambda Functions.

        Parameters
        ----------
        event : dict
            Key 'datasets', list with prefixs descriptions on quicksights datasets.

        Returns
        -------
        Response message
    """
    tables_prefix = ddatasets['datasets']
    ldatasets = wr.quicksight.list_datasets()

    ldatasets_update = []
    for dataset in ldatasets:    
        if any(x in dataset['Name'] for x in tables_prefix):
            arn = dataset['Arn']
            dataset_id = arn.split("/")[1]

            ldatasets_update.append(dataset_id)
    
    response = create_ingestion(account_id=settings.AWS_ACCOUNT_ID, ldatasets_ids=ldatasets_update)
    
    if response["StatusCode"] == 200:
        return { "StatusCode": 200, "message": "Ingestion created successfully" }
    else:
        return { "StatusCode": 500, "message": "ERROR while trying to create ingestion" }