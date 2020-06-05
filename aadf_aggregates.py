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
        



    
sc_aadf = spark.read.csv('/home/workspace/uk-data/ukTrafficAADF.csv',header=True,sep=",")
sc_adf = sc_aadf.select('*')
    
#### DROP COLUMNS WITH MORE THAN 10,000 NULL VALUES #####
##### AND AFTER THAT REMOVE NULL VALUES FROM THE DF #####
drop_items = []
for col in sc_adf.columns:
    null_length = sc_adf.select(col).filter(F.col(col).isNull()).count()
    none_length = sc_adf.select(col).filter(F.col(col)== 'None').count()
    if (null_length >= 10000) or (none_length >= 10000):
        #print(col, ':', null_length, '  ', none_length)
        drop_items.append(col)
            
sc_adf = sc_adf.drop(*drop_items)
sc_adf = sc_adf.na.drop()
    
int_cols = ['AADFYear', 'LinkLength_km', 'LinkLength_miles', 'PedalCycles', 'Motorcycles', 'CarsTaxis', 'BusesCoaches', \
            'LightGoodsVehicles', 'V2AxleRigidHGV', 'V3AxleRigidHGV', 'V4or5AxleRigidHGV', \
            'V3or4AxleArticHGV', 'V5AxleArticHGV', 'V6orMoreAxleArticHGV', 'AllHGVs', 'AllMotorVehicles']
    
for col_name in int_cols:
    sc_adf = sc_adf.withColumn(col_name, F.col(col_name).cast('int'))
        
vehicle_columns = ['PedalCycles', 'Motorcycles', 'CarsTaxis', 'BusesCoaches', \
                   'LightGoodsVehicles', 'V2AxleRigidHGV', 'V3AxleRigidHGV', 'V4or5AxleRigidHGV', \
                   'V3or4AxleArticHGV', 'V5AxleArticHGV', 'V6orMoreAxleArticHGV', 'AllHGVs', 'AllMotorVehicles']
    
##Get the miles driven by each vehicle per year,  the vehicles area the aadf measurements for each specific road
### This is sometimes referred to as the volume of traffic  ###
for x in vehicle_columns:
    sc_adf = sc_adf.withColumn( x + '_MilesDriven', ((F.col(x) * F.col('LinkLength_miles') * 365)))
    
#avoid unnecessary duplicates in the roadcategory columns
sc_adf = sc_adf.withColumn('RoadCategory', regexp_replace("RoadCategory", "Pu", "PU"))
sc_adf = sc_adf.withColumn('RoadCategory', regexp_replace("RoadCategory", "Tu", "TU"))
    
#create a new column to identify the road grades: Motorways OR Trunk/Principal Roads
sc_adf = sc_adf.withColumn('Road_Grades', regexp_replace("Road", "[^AM]", ""))
    
#Roads with A and M (AM) --> means former dual carriageway (e.g. A1) has been upgraded to motorway status M
sc_adf = sc_adf.withColumn('Road_Grades', regexp_replace("Road_Grades", "AM|M", "Motorways"))
sc_adf = sc_adf.withColumn('Road_Grades', regexp_replace("Road_Grades", "A", "Trunk/Principal Road"))
    
    
###lets create a road dimension table based on the aadf dataset###
road_table = sc_adf.select('Road', 'RoadCategory', 'Road_Grades').dropDuplicates()
    
    
### AGGREGATE THE AADF AND MILES DRIVEN PER YEAR ###
#### GET THE AVERAGE AADF AND MILES DRIVEN FOR ALL VEHICLES BASED ON EACH ROAD PER YEAR-LOCAL AUTHORITY-REGION #####
### PEDAL CYCLE TABLE ##
pedal_cycle = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', 'Road').agg(mean('PedalCycles').alias('AverageAADF_PC'),
                                                                                 mean('PedalCycles_MilesDriven').alias('AverageMiles_Driven_PC'))
    
##partition by 'Local Authority', order by -- 'AADFYear' and 'Averae AADF' -- this give an orderly table
order_spec = Window.partitionBy("LocalAuthority").orderBy(desc("AADFYear"), desc('AverageAADF_PC'))
pedal_cycle = pedal_cycle.withColumn("row_number", row_number().over(order_spec))
pedal_cycle = pedal_cycle.drop('row_number')
    
#Drop rows where either the average aadf or the miles driven are 0
pedal_cycle = pedal_cycle.filter((F.col('AverageAADF_PC') != 0.0) | (F.col('AverageMiles_Driven_PC') != 0.0))
    
pedal_cycle.show(10)
    
    
### MOTOR VEHICLES TABLE ###
##motor vehicles i.e. motorcycles -- MC, carstaxis -- CT, busescoaches -- BC, light vans -- LV
motor_vehicles = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', 'Road').agg(mean('Motorcycles').alias('AverageAADF_MC'),
                                                                                    mean('CarsTaxis').alias('AverageAADF_CT'),
                                                                                    mean('BusesCoaches').alias('AverageAADF_BC'),
                                                                                    mean('LightGoodsVehicles').alias('AverageAADF_LV'),
                                                                                    mean('Motorcycles_MilesDriven').alias('AverageMiles_Driven_MC'),
                                                                                    mean('CarsTaxis_MilesDriven').alias('AverageMiles_Driven_CT'),
                                                                                    mean('BusesCoaches_MilesDriven').alias('AverageMiles_Driven_BC'),
                                                                                    mean('LightGoodsVehicles_MilesDriven').alias('AverageMiles_Driven_LV'))
    
    
#arrange the table
order_spec = Window.partitionBy("LocalAuthority").orderBy(desc("AADFYear"))
motor_vehicles = motor_vehicles.withColumn("row_number", row_number().over(order_spec))
motor_vehicles = motor_vehicles.drop('row_number')
    
##Drop rows with 0 aadf/mile driven values
motor_vehicles = motor_vehicles.filter((F.col('AverageAADF_MC') != 0.0) | (F.col('AverageMiles_Driven_MC') != 0.0) \
                                       |(F.col('AverageAADF_CT') != 0.0) | (F.col('AverageMiles_Driven_CT') != 0.0) \
                                       |(F.col('AverageAADF_BC') != 0.0) | (F.col('AverageMiles_Driven_BC') != 0.0) \
                                       |(F.col('AverageAADF_LV') != 0.0) | (F.col('AverageMiles_Driven_LV') != 0.0))
    
motor_vehicles.show(10)    
    
## All Motor vehicles table
all_motorvehicles = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', \
                                   'Road').agg(mean('AllMotorVehicles').alias('AverageAADF_AMV'),
                                               mean('AllMotorVehicles_MilesDriven').alias('AverageMiles_Driven_AMV'))
    
#arrange table
all_motorvehicles = all_motorvehicles.withColumn("row_number", row_number().over(order_spec))
all_motorvehicles = all_motorvehicles.drop('row_number')
all_motorvehicles = all_motorvehicles.filter((F.col('AverageAADF_AMV') != 0.0) | (F.col('AverageMiles_Driven_AMV') != 0.0))
all_motorvehicles.show(10)
    
    
# RIGID AXLE HEAVY GOOD VEHICLES TABLE ##
RHGV = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', 'Road').agg(mean('V2AxleRigidHGV').alias('AverageAADF_HGVR2'), \
                                                                          mean('V3AxleRigidHGV').alias('AverageAADF_HGVR3'),
                                                                          mean('V4or5AxleRigidHGV').alias('AverageAADF_HGVR45'),
                                                                          mean('V2AxleRigidHGV_MilesDriven').alias('AverageMiles_Driven_HGVR2'),
                                                                          mean('V3AxleRigidHGV_MilesDriven').alias('AverageMiles_Driven_HGVR3'), \
                                                                          mean('V4or5AxleRigidHGV_MilesDriven').alias('AverageMiles_Driven_HGVR45'))

#partition by localauthority and order by AADFYear
RHGV = RHGV.withColumn("row_number", row_number().over(order_spec))
RHGV = RHGV.drop('row_number')
#we drop rows where either the average aadf or the miles driven are 0
RHGV = RHGV.filter((F.col('AverageAADF_HGVR2') != 0.0) | (F.col('AverageMiles_Driven_HGVR2') != 0.0) \
                   |(F.col('AverageAADF_HGVR3') != 0.0) | (F.col('AverageMiles_Driven_HGVR3') != 0.0) \
                   |(F.col('AverageAADF_HGVR45') != 0.0) | (F.col('AverageMiles_Driven_HGVR45') != 0.0))

RHGV.show(10)

## AXLE ARTICULATED HEAVY GOOD VEHICLES TABLE ##
    
AHGV = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', 'Road').agg(mean('V3or4AxleArticHGV').alias('AverageAADF_HGVA34'), \
                                                                          mean('V5AxleArticHGV').alias('AverageAADF_HGVA5'),
                                                                          mean('V6orMoreAxleArticHGV').alias('AverageAADF_HGVA6'),
                                                                          mean('V3or4AxleArticHGV_MilesDriven').alias('AverageMiles_Driven_HGVA34'),
                                                                          mean('V5AxleArticHGV_MilesDriven').alias('AverageMiles_Driven_HGVA5'), \
                                                                          mean('V6orMoreAxleArticHGV_MilesDriven').alias('AverageMiles_Driven_HGVA6'))

#partition by localauthority and order by AADFYear
AHGV = AHGV.withColumn("row_number", row_number().over(order_spec))
AHGV = AHGV.drop('row_number')
#we drop rows where either the average aadf or the miles driven are 0
AHGV = AHGV.filter((F.col('AverageAADF_HGVA34') != 0.0) | (F.col('AverageMiles_Driven_HGVA34') != 0.0) \
                   |(F.col('AverageAADF_HGVA5') != 0.0) | (F.col('AverageMiles_Driven_HGVA5') != 0.0) \
                   |(F.col('AverageAADF_HGVA6') != 0.0) | (F.col('AverageMiles_Driven_HGVA6') != 0.0))

AHGV.show(5)
    
## ALL HEAVY GOOD VEHICLES TABLE ##
AllHGV = sc_adf.groupby('AADFYear', 'LocalAuthority', 'Region', 'Road').agg(mean('AllHGVs').alias('AverageAADF_AllHGV'),
                                                                                mean('AllHGVs_MilesDriven').alias('AverageMiles_Driven_AllHGV'))
#patition by localauthority and order by AADFYear
AllHGV = AllHGV.withColumn("row_number", row_number().over(order_spec))
AllHGV = AllHGV.drop('row_number')
#we drop rows where either the average aadf or the miles driven are 0
AllHGV = AllHGV.filter((F.col('AverageAADF_AllHGV') != 0.0) | (F.col('AverageMiles_Driven_AllHGV') != 0.0))
    
##allHGV
AllHGV.show(10)
    
    
#write the tables into csv format
road_table.toPandas().to_csv('/home/workspace/uk-traffic/road_table.csv', index = False, header = None)
pedal_cycle.toPandas().to_csv('/home/workspace/uk-traffic/pedal_cycle.csv', index = False, header = None)
motor_vehicles.toPandas().to_csv('/home/workspace/uk-traffic/motor.csv', index = False, header = None)
all_motorvehicles.toPandas().to_csv('/home/workspace/uk-traffic/allmotor.csv', index = False, header = None)
RHGV.toPandas().to_csv('/home/workspace/uk-traffic/rhgv.csv', index = False, header = None)
AHGV.toPandas().to_csv('/home/workspace/uk-traffic/ahgv.csv', index = False, header = None)  
AllHGV.toPandas().to_csv('/home/workspace/uk-traffic/allhgv.csv', index = False, header = None)