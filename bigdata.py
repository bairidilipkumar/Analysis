# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------


df=spark.read.format('csv').option('inferschema',True).option('header',True).load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

data_schema=StructType([
  StructField('Item_Identifier',StringType(),True),
  StructField('Item_Weight',DoubleType(),True),
  StructField('Item_Fat_Content',StringType(),True),
  StructField('Item_Visibility',DoubleType(),True),
  StructField('Item_Type',StringType(),True),
  StructField('Item_MRP',DoubleType(),True),
  StructField('Outlet_Identifier',StringType(),True),
  StructField('Outlet_Establishment_Year',IntegerType(),True),
  StructField('Outlet_Size',StringType(),True),
  StructField('Outlet_Location_Type',StringType(),True),
  StructField('Outlet_Type',StringType(),True),
  StructField('Item_Outlet_Sales',DoubleType(),True)
])

# COMMAND ----------

df=spark.read.format('csv').option('inferschema',True).option('header',True).schema(data_schema).load('/FileStore/tables/BigMart_Sales-1.csv')

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(col('Item_Identifier'),col('Item_weight')).display()

# COMMAND ----------

 df.select(col('Item_Identifier').alias('item-Number'),col('Item_weight')).display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

from matplotlib import pyplot as plt
import seaborn as sns
import pandas as pd
%matplotlib inline

# COMMAND ----------

da=df.select(col('Item_Fat_Content'))
data=da.toPandas()
sns.set(rc={'figure.figsize':(10,5)})
s=sns.countplot(x='Item_Fat_Content',data=data,)
for bars in s.containers:
   s.bar_label(bars)


# COMMAND ----------

df.filter( (col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10) ).display()

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1','Tier 2'))).display()


# COMMAND ----------

df.withColumn('new-column',lit('flag')).display()

# COMMAND ----------

df.drop(col('new-column'))

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Regular",'Reg'))\
           .withColumn('Item_Fat_Content',regexp_replace(col('Item_Fat_Content'),"Low Fat",'LF')).display()

# COMMAND ----------

df.dropna('any').display()

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df=df.withColumn('curr-date',current_date())
df.display()


# COMMAND ----------

df=df.withColumn('week-after',date_add('curr-date',7))
df.display()

# COMMAND ----------

df=df.withColumn('datediff',datediff('week-after','curr-date'))
df.display()

# COMMAND ----------

df=df.withColumn('curr-date',date_format('curr-date','dd-mm-yyyy'))
df.display()

# COMMAND ----------

gd=df.groupBy('Item_Type').agg(sum('Item_MRP'))
gd.display()

# COMMAND ----------

gd=df.groupBy('Item_Type').agg(avg('Item_MRP'))
gd.display()

# COMMAND ----------

gd=df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP'))
gd.display()

# COMMAND ----------

gd=df.groupBy('Item_Type','Outlet_Size').agg(sum('Item_MRP').alias('sum-of-mrp'),avg('Item_MRP').alias('avg-of-mrp'))
gd.display()

# COMMAND ----------

df=df.withColumn('veg_flag',when(col('Item_Type')=='Meat','No-veg').otherwise('veg'))
df.display()

# COMMAND ----------

df.withColumn('veg_exp_flag',when(((col('veg_flag')=='veg') & (col('Item_MRP')<100)),'veg-not-expensive')\
                              .when((col('veg_flag')=='veg') & (col('Item_MRP')>100),'veg-expensive')\
                              .otherwise('non-veg')).display()

# COMMAND ----------


