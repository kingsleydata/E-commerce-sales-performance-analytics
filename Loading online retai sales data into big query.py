#!/usr/bin/env python
# coding: utf-8

# In[1]:


from google.cloud import bigquery
import pandas
import os
import pandas_gbq
from datetime import datetime, timedelta
import numpy as np


# In[2]:


import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import inspect
import os
import pyodbc


# In[3]:


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'C:\Users\User\Downloads\campaign-analytics-project-08db9f0345f3.json'


# In[4]:


client = bigquery.Client(location="US", project = 'campaign-analytics-project')
print("Client creating using default project: {}".format(client.project))


# In[8]:


online_retail_sales = pd.read_csv(r'C:\Users\User\Downloads\online_retail_II.csv\online_retail_utf_8.csv', encoding = 'utf-8')
online_retail_sales


# In[9]:


online_retail_sales.dtypes


# In[12]:


online_retail_sales['Customer ID'] = online_retail_sales['Customer ID'].fillna(0).astype(int)


# In[17]:


online_retail_sales.tail(5)


# In[25]:


online_retail_sales['InvoiceDate'] = pd.to_datetime(online_retail_sales['InvoiceDate'], format='%m/%d/%y %H:%M')
online_retail_sales


# In[27]:


online_retail_sales.columns = online_retail_sales.columns.str.replace(' ', '_')
online_retail_sales


# In[28]:


pandas_gbq.to_gbq(online_retail_sales, 'ecommerce_ad_dataset.online_retail_sales', project_id = 'campaign-analytics-project', if_exists= 'replace')


# In[ ]:




