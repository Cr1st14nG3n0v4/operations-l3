#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#"""
#Configuración con buckets de lectura y escritura.
#"""

# *Reviews Info: RAPPI
L2_RAPPI_USERS_S3 = 's3://l2-sales-channels/reviews_by_orders/'

# *Orders Detail PopApp
L2_ORDER_DETAIL_POPAPP_S3 = 's3://l2-sales-channels/orders_detail_popapp/'

# PG Info()
PG_HOST = "database-pg-dev.cli5dw0thkjq.us-east-1.rds.amazonaws.com"
PG_USERNAME = "bidev"
PG_PASSWORD = "3xgcLf6ghWrZSyN7"
PG_DATABASE = "bidev"
PG_PORT = 5432

# SLOT TIME params
EARLY = 8
BREAKFAST = 11
LUNCH = 16
AFTERNOON = 19

# AWS_ACCOUNT_ID
AWS_ACCOUNT_ID = "984752346791"

# "Put all ingredients" se observa solamente en Score = 4 al igual que "Ingredient's quality"
DOPTIONS = { 
    "Missing item(s)": "It wasn't what I ordered (missing items[s])",
    "It wasn't what I ordered": "It wasn't what I ordered (missing items[s])",
    "Great experience": "Exactly as I ordered (great experience)",
    "Exactly as I ordered": "Exactly as I ordered (great experience)",
    "Food quality": "Food & Ingredients quality",
    "Ingredient's quality": "Food & Ingredients quality",
    "Put all ingredients": "Food & Ingredients quality"
}