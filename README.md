[//]: # (Image References)

[image3]: ./gad_imgs/roads.jpg "roads"
[image1]: ./gad_imgs/UK_Accident_Data_Model.png "uk schema"
[image2]: ./gad_imgs/uk_Traffic_Agg_Tables.png "uk agg"


# UK_Accident_Traffic_ETL_Pipeline

This is a capstone project that entails building an end-to-end ETL (Extract-Transform-Load) Data pipeline which extracts UK accident and traffic data from Amazon S3, clean and transform with Pyspark, transfer it back to S3 and finally load to Amazon Redshift (Distributed Database), from where the data can be queried for ad-hoc analyses.


![roads][image3]



The Datasets are United Kingdom (UK) Accident Datasets from 2005 to 2011 (excluding year 2009) and Traffic Dataset from 2000 to 2015 for only major roads i.e. A-class roads and Motorways, additional datasets were added to give more meaning to some of the columns in the datasets. The dataset can be found on **[Kaggle ](https://www.kaggle.com/daveianhickey/2000-16-traffic-flow-england-scotland-wales/data).** Informations and descriptions regarding the columns can be found in the **[metadata](all-traffic-data-metadata.pdf).**


### Major Higlights

- First of all, The motivation behind this project was to use ***big data frameworks and cloud services*** to build or develop an ETL pipeline that can be ran automatically without human involvenment based on scheduled time intervals.

- There are two major goals to achieved in this project:


- First, is to create a **STAR SCHEMA with fact and dimension tables** from the UK Accident Dataset (approx. 1.5 million rows) and this would include a **accident fact table** that has the actual accident accurences and casualties that can be extracted from the dataset and **dimension tables** that highlights ***causes of the accident, location, time, conditions of the road, road types, and features that can be attributed to the accident.***


![uk schema][image1]


- Second, is to create **Aggregate tables** from the Traffic Datasets, where each table represent **a type of vehicle** that is aggregated based off **AADF and Miles Driven per year by each type of vehicle on all specific roads from the dataset.** Note: The traffic dataset only contains major roads.

***What is AADF:  AADF (Annual average daily flow) An AADF is the average over a full year of the number of vehicles (in our case of a particular kind of vehicle i.e. numbers of motor vehicles or numbers of heavy goods vehicles) passing a point in the road network each day.***

***Miles Driven: is the number of miles travelled by a vehicle. One vehicle multiplied by one mile travelled (vehicle miles are calculated by multiplying the AADF by the corresponding length of road). For example, one vehicle travelling one mile a day for a year would be 365 vehicle miles. This is sometimes referred to as the volume of traffic.***


![uk agg][image2]



#### To execute each of the above goals, an ETL pipeline was designed and executed:

- Datasets were downloaded from Kaggle and re-uploaded to Amazon S3
- Pyspark perform transformation on the datasets
- After transformation, data are transferred back to Amazon S3
- Tables are created for the Databases
- Data are loaded from S3 to their respective tables in Amazon Redshift (Distributed Database)
- Data quality checks is done on each table to make sure all tables are filled 
- Execution Ends.
- Note: Airflow DAG runs the whole process


- **The ETL process is scheduled to run monthly, as it is expected after every month, new traffic and accident data will be uploaded to Amazon S3.**
- Every month, the whole process starts again. 
- Automation is achieved with ***Apache Airflow***
