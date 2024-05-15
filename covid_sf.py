
# Dataframe Complete Guide (with COVID-19 Dataset)
# Spark which is one of the most used tools when it comes to working with Big Data.
# In this notebook, We will learn standard Spark functionalities needed to work with DataFrames, and finally some tips to handle the inevitable errors you will face.
# I'm going to skip the Spark Installation part for the sake of the notebook, so please go to [Apache Spark Website](http://spark.apache.org/downloads.html) to install Spark that are right to your work setting.


import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import snowflake.snowpark
from snowflake.snowpark import Session

from snowflake.snowpark.functions import * 
from snowflake.snowpark.types import * 


# Initiate the Spark Session
session = Session.builder.app_name('covid-example').getOrCreate()


session

# ## Data
#  We will be working with the Data Science for COVID-19 in South Korea, which is one of the most detailed datasets on the internet for COVID.


# Data can be found in this kaggle URL [Link](https://www.kaggle.com/kimjihoo/coronavirusdataset)


# ### 1. Basic Functions


# #### [1] Load (Read) the data

cases = session.read \
            .option("FIELD_DELIMITER",",") \
            .option("INFER_SCHEMA",True) \
            .option("PARSE_HEADER",True) \
            .csv("@DATA/Case.csv")

# First few rows in the file
cases.show()


# It looks ok right now, but sometimes as we the number of columns increases, the formatting becomes not too great. I have noticed that the following trick helps in displaying in pandas format in my Jupyter Notebook. 
# 
# The **.toPandas()** function converts a **Spark Dataframe** into a **Pandas Dataframe**, which is much easier to play with.


cases.limit(10).toPandas()


# #### [2] Change Column Names


# To change a single column,


cases = cases.withColumnRenamed("infection_case","infection_source")


# To change all columns,


cases = cases.toDF(*['case_id', 'province', 'city', 'group', 'infection_case', 'confirmed',
       'latitude', 'longitude'])


cases.show()


# #### [3] Change Column Names


# We can select a subset of columns using the **select**


cases = cases.select('province','city','infection_case','confirmed')
cases.show()


# #### [4] Sort by Column


# Simple sort
cases.sort("confirmed").show()


# Descending Sort
from snowflake.snowpark import functions as F

cases.sort(F.desc("confirmed")).show()
