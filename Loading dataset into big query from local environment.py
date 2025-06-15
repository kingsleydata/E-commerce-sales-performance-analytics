#!/usr/bin/env python
# coding: utf-8

#importing libraries
from google.cloud import bigquery
import pandas_gbq
from datetime import datetime, timedelta
import numpy as np
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import os
import pyodbc

#Connecting with bigquery project key
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\User\Downloads\campaign-analytics-project-08db9f0345f3.json'

#Connecting with bigquery dataset
client = bigquery.Client(location="US", project = 'campaign-analytics-project')
print("Client creating using default project: {}".format(client.project))


#Reading dataset from local environment
online_retail_sales = pd.read_csv(r'C:\Users\User\Downloads\online_retail_II.csv\online_retail_utf_8.csv', encoding = 'utf-8')


#Checking for data type
online_retail_sales.dtypes

#Filling missing rows with a default number 0, unregistered customers are not assigned a 
#unique customer id number so we give unregistered customers a default ID number of 0
online_retail_sales['Customer ID'] = online_retail_sales['Customer ID'].fillna(0).astype(int)

#Converting to the date column to the proper date format and data type
online_retail_sales['InvoiceDate'] = pd.to_datetime(online_retail_sales['InvoiceDate'], format='%m/%d/%y %H:%M')
online_retail_sales


#Replacing all columns having space within with _ (underscore)
online_retail_sales.columns = online_retail_sales.columns.str.replace(' ', '_')
online_retail_sales

#Loading the data from local environment into BigQuery for analysis
pandas_gbq.to_gbq(online_retail_sales, 'ecommerce_ad_dataset.online_retail_sales', project_id = 'campaign-analytics-project', if_exists= 'replace')

