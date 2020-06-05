from datetime import datetime
from configparser import ConfigParser
import os
import pyspark
import copy
from functools import reduce
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import Window
from pyspark.sql.functions import row_number, desc, col, when, udf, concat, lit, to_timestamp, to_date, countDistinct
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, from_unixtime, dayofweek
from pyspark.sql.functions import monotonically_increasing_id, regexp_replace, mean
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl, StringType as Str, IntegerType as Int, DateType as Dat, TimestampType, LongType




spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
        
        
        
        
def accident_fact_dimension():
    
    #Download all the required csv files from S3
    sc_2005 = spark.read.csv('/home/workspace/uk-data/accidents_2005_to_2007.csv',header=True,sep=",")
    sc_2011 = spark.read.csv('/home/workspace/uk-data/accidents_2009_to_2011.csv',header=True,sep=",")
    sc_2014 = spark.read.csv('/home/workspace/uk-data/accidents_2012_to_2014.csv',header=True,sep=",")
    sc_LAD = spark.read.csv('/home/workspace/uk-data/LAD.csv',header=True,sep=",")
    sc_LAD3 = spark.read.csv('/home/workspace/uk-data/LAD3.csv',header=True,sep=",")
    sc_LAD4 = spark.read.csv('/home/workspace/uk-data/LAD4.csv',header=True,sep=",")
    
    #Combine the accident data
    def unionAll(*dfs):
        return reduce(DataFrame.unionAll, dfs)
    
    sc_accidents = unionAll(sc_2005, sc_2011, sc_2014)
    
    ##Convert Columns to their respective data types
    int_cols = ['Location_Easting_OSGR', 'Location_Northing_OSGR', 'Police_Force', 'Accident_Severity', \
                'Number_of_Vehicles', 'Number_of_Casualties', 'Urban_or_Rural_Area', '1st_Road_Number', '2nd_Road_Number']
    
    float_cols = ['Longitude', 'Latitude']
    
    for col_name in int_cols:
        sc_accidents = sc_accidents.withColumn(col_name, F.col(col_name).cast('int'))
        
    for col_name in float_cols:
        sc_accidents = sc_accidents.withColumn(col_name, F.col(col_name).cast('float'))
    
    #copy the dataframe into another one
    sc_uk = sc_accidents.select("*")
        
    #### DROP COLUMNS WITH MORE THAN 10,000 NULL VALUES #####
    ##### AND AFTER THAT REMOVE NULL VALUES FROM THE DF #####
    drop_items = []
    for col in sc_uk.columns:
        null_length = sc_uk.select(col).filter(F.col(col).isNull()).count()
        none_length = sc_uk.select(col).filter(F.col(col)== 'None').count()
        if (null_length >= 10000) or (none_length >= 10000):
            #print(col, ':', null_length, '  ', none_length)
            drop_items.append(col)
            
    sc_uk = sc_uk.drop(*drop_items)
    sc_uk = sc_uk.na.drop()
    
    #create a new column called date_time
    sc_uk = sc_uk.withColumn('date_time', F.concat(F.col("Date"), F.lit(" "), F.col("Time")))
    
    #Drop rows if the accidents have the same time and position more than ONCE, that means it is a duplicate
    sc_uk = sc_uk.dropDuplicates(['date_time', 'Longitude', 'Latitude'])
    
    #### CREATE LOCATION DIMENSION TABLE ####
    #Accident Index -- as the primary key for the location table
    loc_table = sc_uk.select('Accident_Index', 'Location_Easting_OSGR', 'Location_Northing_OSGR', 'Longitude', 'Latitude').distinct()
    loc_table.show(10)
    
    ##### CREATE TIME DIMENSION TABLE ######
    #this requires some feature engineering
    sc_uk = sc_uk.drop(*['Date', 'Time'])
    sc_uk = sc_uk.withColumn('date_time', F.from_unixtime(F.unix_timestamp("date_time", "dd/MM/yy HH:mm")).cast('timestamp'))
    sc_uk = sc_uk.withColumn('Day_of_Week', dayofweek('date_time'))
    sc_uk = sc_uk.withColumn('Year', year('Year'))
    sc_uk = sc_uk.withColumn('Month', month(to_date("date_time","MM/dd/yyyy")))
    sc_uk = sc_uk.withColumn('week_of_year', weekofyear(to_date('date_time')))
    sc_uk = sc_uk.withColumn("week_of_month", date_format(to_date("date_time", "dd/MMM/yyyy"), "W"))
    
    time_table = sc_uk.select('date_time', 'Day_of_Week', 'week_of_year', 'week_of_month', 'Month', 'Year').dropDuplicates()
    ##Time Table
    time_table.show(10)
    
    ###### CREATE LOCAL AUTHORITY DIMENSION TABLE ######
    sc_uk = sc_uk.withColumnRenamed("Local_Authority_(Highway)","LAD_Code")
    sc_LAD = sc_LAD.withColumnRenamed("Supergroup name","Geographical Area") \
    .withColumnRenamed("Code","LAD_Code")
    #join uk accident DF with the LAD DF
    sc_uk = sc_uk.join(sc_LAD,["LAD_Code"],how='inner')
    sc_LAD2 = sc_LAD3.join(sc_LAD4,["Sub-Group"],how='inner')
    sc_LAD2 = sc_LAD2.withColumnRenamed("ONS Code","LAD_Code")
    sc_uk = sc_uk.join(sc_LAD2,["LAD_Code"],how='inner')
    sc_uk = sc_uk.drop('Name')
    
    #LAD table
    LAD_table = sc_uk.select("LAD_Code", F.col('Sub-Group').alias('SubGroup'), F.col('Highway Authority').alias('HighwayAuthority'),
                              F.col('Sub-Group Description').alias('SubGroup_Description'), F.col('Super-Group').alias('SuperGroup'),
                              F.col('Network Density Range').alias('Network_Density_Range'), F.col('Urban Road Range').alias('Urban_Road_Range'),
                              F.col('Region/Country').alias('Region_Country'), F.col('Geographical Area').alias('Geographical_Area')).distinct()
    LAD_table.show(10)
    
    
    #### CREATE CONDITION DIMENSION TABLE#####
    #consist of 'Light_Conditions', 'Weather_Conditions', 'Road_Surface_Conditions'
    
    ##Change "Other" value in Weather Condition table to "Unknown"-
    sc_uk = sc_uk.withColumn("Weather_Conditions",regexp_replace("Weather_Conditions", "Other", "Unknown"))
    
    #create a column known as conditions that consist of all the conditions
    sc_uk = sc_uk.withColumn('conditions', F.concat(F.col('Light_Conditions'), \
                                                      F.lit(" and "), F.col('Weather_Conditions'),
                                                      F.lit(" and "), F.col('Road_Surface_Conditions')))
    
    #Extract values from columns
    def extract_values(df, column):
        values = df.select(column).distinct().collect()
        values = [x[0] for x in values]
        return values
    
    values = extract_values(sc_uk, 'conditions')
    
    #this function will be used to create primary id for dimension tables
    def new_column(column):
        if column in values:
            return values.index(column) + 1
    udfValue = udf(new_column, Int())
    
    sc_uk = sc_uk.withColumn('Condition_Index', udfValue('conditions'))
    
    #condition table
    condition_table = sc_uk.select("Condition_Index", 'Weather_Conditions', 
                                    'Light_Conditions', 'Road_Surface_Conditions').distinct()
    
    condition_table.sort('Condition_Index').show(10)
    
    #### CREATE ROAD DIMENSION TABLE #####
    #'1st_Road_Class', 'Road_Type', '2nd_Road_Class'
    sc_uk = sc_uk.withColumn('road_classification',
                             F.concat(F.col('1st_Road_Class'), F.lit(" and "), 
                                      F.col('Road_Type'), F.lit(" and "), F.col('2nd_Road_Class')))
    values = extract_values(sc_uk, 'road_classification')
    udfValue = udf(new_column, Int())
    sc_uk = sc_uk.withColumn('Road_Index', udfValue('road_classification'))
    
    #road table
    road_table = sc_uk.select('Road_Index', '1st_Road_Class', 'Road_Type', '2nd_Road_Class').distinct()
    road_table.sort('Road_Index').show(5)
    
    #### CREATE PEDESTRIAN DIMENSION TABLE #####
    # Pedestrian_Crossing-Human_Control, Pedestrian_Crossing-Physical_Facilities
    sc_uk = sc_uk.withColumn('pedestrian', 
                             F.concat(F.col('Pedestrian_Crossing-Human_Control'), 
                                      F.lit(" and "), F.col('Pedestrian_Crossing-Physical_Facilities')))
    values = extract_values(sc_uk, 'pedestrian')
    udfValue = udf(new_column, Int())
    sc_uk = sc_uk.withColumn('Pedestrian_Index', udfValue('pedestrian'))
    
    #pedestrian table
    pedestrian_table = sc_uk.select('Pedestrian_Index', 'Pedestrian_Crossing-Human_Control', 
                                     'Pedestrian_Crossing-Physical_Facilities').distinct()
    pedestrian_table.sort('Pedestrian_Index').show(10)
    
    #drop the features used to create ids for the dimension table
    drop_list = ['conditions', 'road_classification', 'pedestrian']
    sc_uk = sc_uk.drop(*drop_list)
    
    
    ##### CREATE ACCIDENT TABLE --- FACT TABLE #####
    #ACCIDENT INDEX -- PRIMARY KEY FOR LOCATION TABLE
    #LAD_CODE -- PRIMARY KEY FOR LOCAL AUTHORITY DISTRICT TABLE
    #DATE TIME -- PRIMARY KEY FOR TIME TABLE
    #CONDITION INDEX -- PRIMARY KEY FOR CONDITIONS TABLE
    #ROAD INDEX -- PRIMARY KEY FOR ROAD TABLE
    #PEDESTRIAN INDEX -- PRIMARY KEY FOR PEDESTRIAN TABLE
    
    accident_fact_table = sc_uk.select('Accident_Index', 'LAD_Code', 
                                       'date_time', 'Year', 'Police_Force', 
                                       'Accident_Severity', 'Number_of_Vehicles', 'Number_of_Casualties', 
                                       '1st_Road_Number', 'Speed_limit', '2nd_Road_Number', 'Urban_or_Rural_Area', 
                                       'Did_Police_Officer_Attend_Scene_of_Accident', 
                                       'Condition_Index', 'Road_Index', 'Pedestrian_Index')
    
    
    #write the tables into csv format
    loc_table.toPandas().to_csv('/home/workspace/uk-accident/loc_table.csv', index = False, header = None)
    time_table.toPandas().to_csv('/home/workspace/uk-accident/time_table.csv', index = False, header = None)
    LAD_table.toPandas().to_csv('/home/workspace/uk-accident/LAD_table.csv', index = False, header = None)
    condition_table.toPandas().to_csv('/home/workspace/uk-accident/condition_table.csv', index = False, header = None)
    pedestrian_table.toPandas().to_csv('/home/workspace/uk-accident/pedestrian_table.csv', index = False, header = None)
    road_table.toPandas().to_csv('/home/workspace/uk-accident/rd_table.csv', index = False, header = None)  
    accident_fact_table.toPandas().to_csv('/home/workspace/uk-accident/accident_fact_table.csv', index = False, header = None) 
    
#main
if __name__ == "__main__":
    accident_fact_dimension()