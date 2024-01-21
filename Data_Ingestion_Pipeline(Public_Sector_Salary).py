#!/usr/bin/env python
# coding: utf-8

# ### A2 - Data Ingestion Pipeline

# In[1]:


import pandas as pd
import sqlalchemy as sa
import requests


# ### Find a data source with an API

# https://data.ontario.ca/dataset/public-sector-salary-disclosure-2020/resource/23172a73-7b85-49bd-9064-d600d2b21d37

# ### Describe briefly the data source

# Information on all public sector employees who were paid $100,000 or more in 2020 and are subject to the Public Sector Salary Disclosure Act.

# ### Illustrate how to prepare a GET request to pull the data.

# From `requests` library in Python, use `.get()` function with the API URL containing specific parameters.

# ### What parameters do you need to provide?

# Parameters such as `resource_id`, `limit`, and `offset`.

# ### What type of authentication do you need to use?

# No Auth

# ### Test the connection to the Data Source using Postman.

# Get the same result in the Postman:
# 
# GET: https://data.ontario.ca/api/3/action/datastore_search?resource_id=23172a73-7b85-49bd-9064-d600d2b21d37&limit=2000&offset=0
# 
# Query Params:
# 
# [Key]--------------[Value]
# 
# * resource_id:  23172a73-7b85-49bd-9064-d600d2b21d37
# * limit:        2000
# * offset:       0

# ### Write a function to request the data

# In[2]:


resource_id = '23172a73-7b85-49bd-9064-d600d2b21d37'
limit = 2000
offset = 0

api_url = 'https://data.ontario.ca/api/3/action/datastore_search?resource_id={}&limit={}&offset={}'.format(resource_id, limit, offset)

print(api_url)


# In[3]:


api_response = requests.get(api_url)
api_response


# In[4]:


data = api_response.json()
data


# In[5]:


data['help']


# In[6]:


data['result']


# In[7]:


data['result']['records']


# In[8]:


# bring to pandas dataframe
salary_cases = pd.DataFrame(data['result']['records'])
salary_cases.tail()


# ### Data Engineering

# In[9]:


salary_cases.describe(include='all')


# In[10]:


salary_cases.info()


# ### Illustrate your Data Cleaning and Feature Engineering processes

# From the statistic description and data info, we can see that some of the columns have wrong datatypes (`Salary`, `Benefits`, `Year`). Therefore, first if all, we need to convert the datatypes of the three columns.
# 
# From the dataset, we notice there are str symbols like `$` and `,`. We need to replace them with np space using `column.str.replace('','')`

# In[11]:


salary_cases['Salary'] = salary_cases['Salary'].str.replace(',', '').str.replace('$', '')


# In[12]:


salary_cases['Benefits'] = salary_cases['Benefits'].str.replace(',', '').str.replace('$', '')


# Then, use `.to_numeric()` function from `pandas` library to covert these columns into numeric values.

# In[13]:


salary_cases['Salary'] = pd.to_numeric(salary_cases['Salary'])
salary_cases['Benefits'] = pd.to_numeric(salary_cases['Benefits'])
salary_cases['Year'] = pd.to_numeric(salary_cases['Year'])


# Create a new column defined as `Award percentage(%)` which is Benefits/Salary.

# In[18]:


salary_cases['Award percentage(%)'] = round(salary_cases['Benefits']/salary_cases['Salary']*100,3)


# In[19]:


salary_cases


# In[20]:


cleaned_salary_data = salary_cases


# In[21]:


cleaned_salary_data.info()


# In[22]:


cleaned_salary_data.describe(include='all')


# ### Ingest the data into the database in the PostgreSQL server.

# In[23]:


db_secret = {
    'drivername' : 'postgresql+psycopg2',
    'host'       : 'mmai5100postgres.canadacentral.cloudapp.azure.com',
    'port'       : '5432',
    'username'   : 'snow1227',
    'password'   : '2023!Schulich',
    'database'   : 'snow1227_db'
}


# In[24]:


db_connection_url = sa.engine.URL.create(
    drivername = db_secret['drivername'],
    username   = db_secret['username'],
    password   = db_secret['password'],
    host       = db_secret['host'],
    port       = db_secret['port'],
    database   = db_secret['database']
)


# In[25]:


engine = sa.create_engine(db_connection_url)


# In[26]:


cleaned_salary_data.to_sql(
    name = 'public_sector_salary',
    schema = 'star',
    con = engine,
    if_exists = 'replace',
    index=False,
    method='multi',
    dtype= {
        ' _id'   : sa.types.INTEGER(),
        'Sector' : sa.types.VARCHAR(255),
        'Last Name' : sa.types.VARCHAR(255),
        'First Name' : sa.types.VARCHAR(255),
        'Salary' : sa.types.DECIMAL(20,2),
        'Benefits' : sa.types.DECIMAL(20,2),
        'Employer' : sa.types.VARCHAR(255),
        'Job title' : sa.types.VARCHAR(255),
        'Year'   : sa.types.INTEGER(),
        'Award percentage(%)': sa.types.DECIMAL(10,3)
    }
)

