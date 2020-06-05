# UK_Accident_Traffic_ETL_Pipeline

This is a capstone project that entails building an end-to-end ETL (Extract-Transform-Load) Data pipeline which extracts UK accident and traffic data from Amazon S3, clean and transform with Pyspark, transfer it back to S3 and finally load to Amazon Redshift (Distributed Database), from where the data can be queried for ad-hoc analyses.


The Datasets are United Kingdom (UK) Accident Datasets from 2005 to 2011 (excluding year 2009) and Traffic Dataset from 2000 to 2015 for only major roads i.e. A-class roads and Motorways. The dataset can be found on **[Kaggle ](https://www.kaggle.com/daveianhickey/2000-16-traffic-flow-england-scotland-wales/data).** Informations and descriptions regarding the columns can be found in the [metadata](other_file.md)
