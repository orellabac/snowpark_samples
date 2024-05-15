
# # PySpark Dataframe Complete Guide (with COVID-19 Dataset)


# Spark which is one of the most used tools when it comes to working with Big Data.


# In this notebook, We will learn standard Spark functionalities needed to work with DataFrames, and finally some tips to handle the inevitable errors you will face.


# I'm going to skip the Spark Installation part for the sake of the notebook, so please go to [Apache Spark Website](http://spark.apache.org/downloads.html) to install Spark that are right to your work setting.


import pandas as pd
import numpy as np
from datetime import date, timedelta, datetime
import time

import pyspark
from pyspark.sql import SparkSession, SQLContext
from pyspark.context import SparkContext
from pyspark.sql.functions import * 
from pyspark.sql.types import * 


# Initiate the Spark Session
spark = SparkSession.builder.appName('covid-example').getOrCreate()


spark


# ## Data
#  We will be working with the Data Science for COVID-19 in South Korea, which is one of the most detailed datasets on the internet for COVID.


# Data can be found in this kaggle URL [Link](https://www.kaggle.com/kimjihoo/coronavirusdataset)


# ### 1. Basic Functions


# #### [1] Load (Read) the data


cases = spark.read.load("/FileStore/Case.csv",
                        format="csv", 
                        sep=",", 
                        inferSchema="true", 
                        header="true")


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
from pyspark.sql import functions as F

cases.sort(F.desc("confirmed")).show()


# #### [5] Change Column Type


from pyspark.sql.types import DoubleType, IntegerType, StringType

cases = cases.withColumn('confirmed', F.col('confirmed').cast(IntegerType()))
cases = cases.withColumn('city', F.col('city').cast(StringType()))

cases.show()


# #### [6] Filter


# We can filter a data frame using multiple conditions using AND(&), OR(|) and NOT(~) conditions. For example, we may want to find out all the different infection_case in Daegu with more than 10 confirmed cases.


cases.filter((cases.confirmed>10) & (cases.province=='Daegu')).show()


# #### [7] GroupBy


from pyspark.sql import functions as F

cases.groupBy(["province","city"]).agg(F.sum("confirmed") ,F.max("confirmed")).show()


# Or if we don’t like the new column names, we can use the **alias** keyword to rename columns in the agg command itself.


cases.groupBy(["province","city"]).agg(
    F.sum("confirmed").alias("TotalConfirmed"),\
    F.max("confirmed").alias("MaxFromOneConfirmedCase")\
    ).show()


# #### [8] Joins


# Here, We will go with the region file which contains region information such as elementary_school_count, elderly_population_ratio, etc.


regions = spark.read.load("/FileStore/Region.csv",
                          format="csv", 
                          sep=",", 
                          inferSchema="true", 
                          header="true")

regions.limit(10).toPandas()


# Left Join 'Case' with 'Region' on Province and City column
cases = cases.join(regions, ['province','city'],how='left')
cases.limit(10).toPandas()


# ### 2. Use SQL with DataFrames


# We first register the cases dataframe to a temporary table cases_table on which we can run SQL operations. As you can see, the result of the SQL select statement is again a Spark Dataframe.
# 
# All complex SQL queries like GROUP BY, HAVING, AND ORDER BY clauses can be applied in 'Sql' function


cases.registerTempTable('cases_table')
newDF = SQLContext.sql('select * from cases_table where confirmed > 100')
newDF.show()


# ### 3. Create New Columns


# There are many ways that you can use to create a column in a PySpark Dataframe.


# #### [1] Using Spark Native Functions


# We can use .withcolumn along with PySpark SQL functions to create a new column. In essence, you can find String functions, Date functions, and Math functions already implemented using Spark functions. Our first function, the F.col function gives us access to the column. So if we wanted to add 100 to a column, we could use F.col as:


import pyspark.sql.functions as F

casesWithNewConfirmed = cases.withColumn("NewConfirmed", 100 + F.col("confirmed"))
casesWithNewConfirmed.show()


# We can also use math functions like F.exp function:


casesWithExpConfirmed = cases.withColumn("ExpConfirmed", F.exp("confirmed"))
casesWithExpConfirmed.show()


# #### [2] Using Spark UDFs


# Sometimes we want to do complicated things to a column or multiple columns. This could be thought of as a map operation on a PySpark Dataframe to a single column or multiple columns. While Spark SQL functions do solve many use cases when it comes to column creation, I use Spark UDF whenever I need more matured Python functionality. \
# 
# To use Spark UDFs, we need to use the F.udf function to convert a regular python function to a Spark UDF. We also need to specify the return type of the function. In this example the return type is StringType()


import pyspark.sql.functions as F
from pyspark.sql.types import *

def casesHighLow(confirmed):
    if confirmed < 50: 
        return 'low'
    else:
        return 'high'
    
#convert to a UDF Function by passing in the function and return type of function
casesHighLowUDF = F.udf(casesHighLow, StringType())
CasesWithHighLow = cases.withColumn("HighLow", casesHighLowUDF("confirmed"))
CasesWithHighLow.show()


# #### [3] Using Pandas UDF


# This allows you to use pandas functionality with Spark. I generally use it when I have to run a groupBy operation on a Spark dataframe or whenever I need to create rolling features
#  
# The way we use it is by using the F.pandas_udf decorator. **We assume here that the input to the function will be a pandas data frame**
# 
# The only complexity here is that we have to provide a schema for the output Dataframe. We can use the original schema of a dataframe to create the outSchema.


cases.printSchema()


from pyspark.sql.types import IntegerType, StringType, DoubleType, BooleanType
from pyspark.sql.types import StructType, StructField

# Declare the schema for the output of our function

outSchema = StructType([StructField('case_id',IntegerType(),True),
                        StructField('province',StringType(),True),
                        StructField('city',StringType(),True),
                        StructField('group',BooleanType(),True),
                        StructField('infection_case',StringType(),True),
                        StructField('confirmed',IntegerType(),True),
                        StructField('latitude',StringType(),True),
                        StructField('longitude',StringType(),True),
                        StructField('normalized_confirmed',DoubleType(),True)
                       ])
# decorate our function with pandas_udf decorator
@F.pandas_udf(outSchema, F.PandasUDFType.GROUPED_MAP)
def subtract_mean(pdf):
    # pdf is a pandas.DataFrame
    v = pdf.confirmed
    v = v - v.mean()
    pdf['normalized_confirmed'] = v
    return pdf

confirmed_groupwise_normalization = cases.groupby("infection_case").apply(subtract_mean)

confirmed_groupwise_normalization.limit(10).toPandas()


# ### 4. Spark Window Functions


# We will simply look at some of the most important and useful window functions available.


timeprovince = spark.read.load("./data/TimeProvince.csv",
                          format="csv", 
                          sep=",", 
                          inferSchema="true", 
                          header="true")

timeprovince.show()


# #### [1] Ranking


# You can get rank as well as dense_rank on a group using this function. For example, you may want to have a column in your cases table that provides the rank of infection_case based on the number of infection_case in a province. We can do this by:


from pyspark.sql.window import Window
windowSpec = Window().partitionBy(['province']).orderBy(F.desc('confirmed'))
cases.withColumn("rank",F.rank().over(windowSpec)).show()


# #### [2] Lag Variables


# Sometimes our data science models may need **lag based** features. For example, a model might have variables like the price last week or sales quantity the previous day. We can create such features using the lag function with window functions. \
# 
# Here I am trying to get the confirmed cases 7 days before. I am filtering to show the results as the first few days of corona cases were zeros. You can see here that the lag_7 day feature is shifted by 7 days.


from pyspark.sql.window import Window

windowSpec = Window().partitionBy(['province']).orderBy('date')

timeprovinceWithLag = timeprovince.withColumn("lag_7",F.lag("confirmed", 7).over(windowSpec))

timeprovinceWithLag.filter(timeprovinceWithLag.date>'2020-03-10').show()


# #### [3] Rolling Aggregations


# For example, we might want to have a rolling 7-day sales sum/mean as a feature for our sales regression model. Let us calculate the rolling mean of confirmed cases for the last 7 days here. This is what a lot of the people are already doing with this dataset to see the real trends.


from pyspark.sql.window import Window

# we only look at the past 7 days in a particular window including the current_day. 
# Here 0 specifies the current_row and -6 specifies the seventh row previous to current_row. 
# Remember we count starting from 0.

# If we had used rowsBetween(-7,-1), we would just have looked at past 7 days of data and not the current_day
windowSpec = Window().partitionBy(['province']).orderBy('date').rowsBetween(-6,0)

timeprovinceWithRoll = timeprovince.withColumn("roll_7_confirmed",F.mean("confirmed").over(windowSpec))

timeprovinceWithRoll.filter(timeprovinceWithLag.date>'2020-03-10').show()


# One could also find a use for **rowsBetween(Window.unboundedPreceding, Window.currentRow)** function, where we take the rows between the first row in a window and the current_row to get running totals. I am calculating cumulative_confirmed here.


from pyspark.sql.window import Window

windowSpec = Window().partitionBy(['province']).orderBy('date').rowsBetween(Window.unboundedPreceding,Window.currentRow)

timeprovinceWithRoll = timeprovince.withColumn("cumulative_confirmed",F.sum("confirmed").over(windowSpec))

timeprovinceWithRoll.filter(timeprovinceWithLag.date>'2020-03-10').show()


# ### 5. Pivot DataFrames


# Sometimes we may need to have the dataframe in flat format. This happens frequently in movie data where we may want to show genres as columns instead of rows. We can use pivot to do this. Here I am trying to get one row for each date and getting the province names as columns.


pivotedTimeprovince = timeprovince.groupBy('date').pivot('province') \
.agg(F.sum('confirmed').alias('confirmed') , F.sum('released').alias('released'))

pivotedTimeprovince.limit(10).toPandas()


# ### 6. Other Opertions


# #### [1] Caching


# Spark works on the lazy execution principle. What that means is that nothing really gets executed until you use an action function like the .count() on a dataframe. And if you do a .count function, it generally helps to cache at this step. So I have made it a point to cache() my dataframes whenever I do a .count() operation.


df.cache().count()


# #### [2] Save and Load from an intermediate step


# When you work with Spark you will frequently run with memory and storage issues. While in some cases such issues might be resolved using techniques like broadcasting, salting or cache, sometimes just interrupting the workflow and saving and reloading the whole dataframe at a crucial step has helped me a lot. This helps spark to let go of a lot of memory that gets utilized for storing intermediate shuffle data and unused caches.


df.write.parquet("data/df.parquet")
df.unpersist()
spark.read.load("data/df.parquet")


# #### [3] Repartitioning


# You might want to repartition your data if you feel your data has been skewed while working with all the transformations and joins. The simplest way to do it is by using:


df = df.repartition(1000)


# Sometimes you might also want to repartition by a known scheme as this scheme might be used by a certain join or aggregation operation later on. You can use multiple columns to repartition using:


df = df.repartition('cola', 'colb','colc','cold')


# Then, we can get the number of partitions in a data frame using:


df.rdd.getNumPartitions()


# You can also check out the distribution of records in a partition by using the glom function. This helps in understanding the skew in the data that happens while working with various transformations.


df.glom().map(len).collect()


# #### [4] Reading Parquet File in Local
# Sometimes you might want to read the parquet files in a system where Spark is not available. In such cases, I normally use the below code:


from glob import glob
def load_df_from_parquet(parquet_directory):
    df = pd.DataFrame()
    for file in glob(f"{parquet_directory}/*"):
        df = pd.concat([df,pd.read_parquet(file)])
    return df


# ### 7. Close Spark Instance


spark.stop()


# Reference:
# >https://medium.com/@rahul_agarwal


