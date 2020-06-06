[//]: # (Image References)

[image3]: ./gad_imgs/roads.jpg "roads"
[image1]: ./gad_imgs/UK_Accident_Data_Model.png "uk schema"
[image2]: ./gad_imgs/uk_Traffic_Agg_Tables.png "uk agg"
[image4]: ./gad_imgs/dag_interface.JPG "dag interface"
[image5]: ./gad_imgs/graph_dag.JPG "dag pipeline"
[image6]: ./gad_imgs/time_frame_dag.JPG "time frame"
[image7]: ./gad_imgs/tree_view_dag.JPG "tree view"
[image8]: ./gad_imgs/redshift_connect.JPG "data check"
[image9]: ./gad_imgs/uk_pics_DB.jpg "etl pipeline"




# UK_Accident_Traffic_ETL_Pipeline

### - Step 1: Scope the Project and Gather Data

This is a capstone project that entails building an end-to-end ETL (Extract-Transform-Load) Data pipeline which extracts UK accident and traffic data from Amazon S3, clean and transform with Pyspark, transfer it back to S3 and finally load to Amazon Redshift (Distributed Database), from where the data can be queried for ad-hoc analyses.

The end goal of this project is to provide ready-made tables that are waiting to be queried for road transport and traffic statistical analyses. 



![roads][image3]


### Data Source

The Datasets are United Kingdom (UK) Accident Datasets from 2005 to 2011 (excluding year 2008) and Traffic Dataset from 2000 to 2015 for only major roads i.e. A-class roads and Motorways, additional datasets were added to give more meaning to some of the columns in the datasets. The dataset can be found on **[Kaggle ](https://www.kaggle.com/daveianhickey/2000-16-traffic-flow-england-scotland-wales/data).** Informations and descriptions regarding the columns can be found in the **[metadata](all-traffic-data-metadata.pdf).**

Also, the latest United Kingdom (UK) traffic data and corresponding metadata can be found on **[UK Traffic Website](https://roadtraffic.dft.gov.uk/).**

### Step 2: Explore and Assess the Data 

#### - [Data Dictionary included](https://github.com/AdeboyeML/UK_Accident_Traffic_ETL_Pipeline/tree/master/Data_dictionary)

The UK Accident Data set contains approximately 1.5 million rows with 32 columns, each of the columns giving different aspect and attributes in the causes of accident, road types and categories, time, date and year of the occurence, accident severity, local authority and region where the accident occurred, police force in attendance and so much more.

The UK Traffic Dataset contains approximately 260,000 rows with about 29 columns focusing majorly on the major roads in the United Kingdom and the columns include measurement of Annual Average daily flow for each type of vehicles (Pedal cycles, Motor cycles, Buses and Coaches, Light vans, Heavy Goods Vehicle, Rigid axle Heavy Goods Vehicle and Articulated axle Heavy Goods Vehicle).

This **[data dictionary folder](https://github.com/AdeboyeML/UK_Accident_Traffic_ETL_Pipeline/tree/master/Data_dictionary)** contains all information you need to know regarding UK transport and traffic system. 

Also **[Highway Authority Network Classification System](http://mast.roadsafetyanalysis.org/wiki/index.php?title=HANCS)** contains a  lot of information regarding the Road Networks in UK.

More information regarding the UK transport and traffic systems can be found on their **[official page: ]( https://data.gov.uk/dataset/cb7ae6f0-4be6-4935-9277-47e5ce24a11f/road-safety-data).**  the file ***‘STATS19 Variable lookup data guide’*** on this page provides a data dictionary for all the columns in the uk traffic system.

- Note: The data was thoroughly explored prior to building an ETL pipeline for this project, the result of the exploration are what made it possible to develop the two python modules: ``aadf_aggregates.py`` (for aggregate tables) and ``accident_fact_dimension.py`` (accident star schema).


### Step 3 - Define the Data Model


- Two major goals were said to be achieved from this project:


- First, is to create a **STAR SCHEMA with fact and dimension tables** from the UK Accident Dataset (approx. 1.5 million rows) and this would include a **accident fact table** that has the actual accident occurences and casualties and **dimension tables** that highlights ***causes of the accident, location, time, conditions of the road, road types, and features that can be attributed to the accident.***


![uk schema][image1]


- Second, is to create **Aggregate tables** from the Traffic Datasets, where each table represent **a type of vehicle** that is aggregated based off **AADF and Miles Driven per year by each type of vehicle on all specific roads from the dataset.** Note: The traffic dataset only contains major roads.

***What is AADF:  AADF (Annual average daily flow) An AADF is the average over a full year of the number of vehicles (in our case of a particular kind of vehicle i.e. numbers of motor vehicles or numbers of heavy goods vehicles) passing a point in the road network each day.***

***Miles Driven: is the number of miles travelled by a vehicle. One vehicle multiplied by one mile travelled (vehicle miles are calculated by multiplying the AADF by the corresponding length of road). For example, one vehicle travelling one mile a day for a year would be 365 vehicle miles. This is sometimes referred to as the volume of traffic.***


![uk agg][image2]

### Step 4: Run ETL to Model the Data

#### To execute each of the above goals, an ETL pipeline was designed and executed:

- Datasets were downloaded from Kaggle and re-uploaded to Amazon S3
- Pyspark perform transformation on the datasets
- After transformation, data are transferred back to Amazon S3
- Tables are created for the Databases
- Data are loaded from S3 to their respective tables in Amazon Redshift (Distributed Database)
- Data quality checks is done on each table to make sure all tables are filled 
- Execution Ends.
- Note: Airflow DAG runs the whole process.


### Architecture

![etl pipeline][image9]




- **The ETL process is scheduled to run monthly, as it is expected after every month, new traffic and accident data will be uploaded to Amazon S3.**
- Every month, the whole process starts again. 
- Automation is achieved with ***Apache Airflow***


### Environmental Setup 
- To set up your Amazon Redshift, you will need to have set in place your IAM Role, IAM User, Security Group, the instruction to set them up including Redshift can be found **[here ](https://github.com/AdeboyeML/UK_Accident_Traffic_ETL_Pipeline/tree/master/AWS_setup).**
- Amazon Redshift --> I utilized ``1 node`` with ``dc2.large``
- Apache Airflow --> I utilized the Udacity Airflow server available for students in ***the project workspace***

### Justification for choice of tools:

- Amazon S3 was chosen as the data storage simply because it's a simple storage service that offers an extremely durable, highly available, and infinitely scalable data storage infrastructure at very low costs. And it provides efficient data transfer to our distributed Database (Amazon Redshift).


- PySpark (Python API for Apache Spark) - is an amazing framework when it comes to working with huge datasets. Whether it is to perform computations on large datasets or to just analyze them, it is the best for real-time computations and fast processing of large datasets.


- Amazon Redshift is a fully managed, petabyte-scale data warehouse service in the cloud, that allow queries to be ran simultaneously by numerous users providing fast query performance using the same SQL-based tools and business intelligence applications.


- Apache Airflow was chosen for the ETL workflow automation because it provides workflow automation and scheduling system that can be used to author and manage data pipelines. The workflows are made of directed acyclic graphs (DAGs) of tasks.



### Airflow Runtime Overview

#### Airflow Interface:

![dag interface][image4]


#### Airflow ETL Workflow:

![dag pipeline][image5]


#### Time Frame:

![time frame][image6]



#### Airflow Tree View:

![tree view][image7]



### Quality Check on one of the tables in Redshift Database

![data check][image8]







### Outlook:
### Step 5: Complete Project Write Up

- ***If the data increased 100x,*** With increase in the traffic and accident data, Amazon EMR clusters will need to be used for the transformation of the data, this is because as of now it takes approx. 40 -50 mins to completely transform the data, but with EMR cluster, things will be done faster.

- ***The pipelines would be run on a daily basis by 7 am every day.*** The ETL pipeline for now is scheduled to be ran monthly, but can also be configured to run everyday at 7am, The only reason for monthly scheduling is to be able to gather as much data as possible from different local authorities for the month.

- ***The database needed to be accessed by 100+ people.*** The more people accessing the database the more cpu resources you need  (on Amazon Redshift) to get a fast experience. By using a distributed database you can improve your replications and partitioning to get faster query results for each user. ***Note: Amazon Redshift has been pre-configured to allow 500 maximum connections established, but only 50 can run in parallel at a point in time.***







***Tools utilized: Python, Pyspark (Apache Spark), Amazon Web Services (s3, redshift), Apache airflow, PostgreSQL***




