# Building an ETL pipeline with Apache Airflow, AWS S3 and Redshift.
***
## Architecture

## What is the data sources ?
Yellow taxi trip records include fields capturing pick-up and drop-off dates/times, pick-up and drop-off locations, trip distances, itemized fares, rate types, payment types, and driver-reported passenger counts. The data used in the attached datasets were collected and provided to the NYC Taxi and Limousine Commission (TLC) by technology providers authorized under the Taxicab & Livery Passenger Enhancement Programs (TPEP/LPEP). The trip data was not created by the TLC, and TLC makes no representations as to the accuracy of these data.

### Data
[Website ](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

[Taxi Zone Shapefile ](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip)


## Data modeling
Once the details for each type of records have been detected, it is easy to know what are the features, entities, and relations of the model. My proposed data model contains the expenses services separated in fact tables, sharing dimensions between these facts, therefore, the proposed model will be a constellation scheme.
![image](Images\modeling.png "data modeling")

## Infastructure as Code (IaC) in AWS
The aim of this section is to create a Redshift cluster on AWS and keep it available for use by the airflow DAG. In addition to preparing the infrastructure, the file AWS-IAC-IAM-EC2-S3-Redshift.ipynb will help you to have an alternative staging zone in S3 as well.

## Building an ETL data pipeline with Apache Airflow
### Docker enviroment
> Install Docker Desktop on Windows, it will install docker compose as well, docker compose will alow you to run multiple containers applications, Apache airflow has three main components: metadata database, scheduler and webserver, in this case we will use a celery executor too.

> Once all the files needed were downloaded from the repository , Let's run everything we will use the git bash tool again, go to the folder Uber-expenses-tracking we will run docker compose command

