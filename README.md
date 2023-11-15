# Spark Cluster with Docker & docker-compose(2021 ver.)

# General

A simple spark standalone instance for testing environment purposses. A *docker-compose up* away from you solution for your spark development environment.

The Docker compose will create the following container:

container|Exposed ports
---|---
spark-master|9090 7077


# Installation

The following steps will make you run your spark container.

## Pre requisites

* Docker installed

* Docker compose  installed

## Current solution focused on

The script for:
- retrieving data (CSV) from AWS S3 into Spark DataFrame;
- making some transformations
- uploading result to AWS S3 according to path defined by partition rule and some additional labels

## Reusability 

You should specify the credentials like in file configs/auth_config.example.yml -> configs/auth_config.yml 

For choosing other settings of SparkContext you should modify the file configs/running_config.yml 

## Build the tests

```sh
docker-compose -f test_docker-compose.yml up --build
```
You should the output of successfull passing tests

## Run the docker-compose (MAIN SOLUTION)

The final step to create your docker instance with Spark Framework based on image 
<b>jupyter/pyspark-notebook:python-3.8.8</b> - stable version of environment for developing

```sh
docker-compose up --build -d
```

## Validate your producing

Please, check log file in log directory, you should see next information

INFO:src.logger:{"level": "info", "time": "2023-11-15T13:02:23.745555", "tag": "s3 configs", "message": "Got next path 2021/01/30/2021-01-30.csv in bucket - bucket_name"}
INFO:src.logger:{"level": "info", "time": "2023-11-15T13:02:44.455570", "tag": "spark init", "message": "Got Spark environment with version: 3.1.1."}
INFO:src.logger:{"level": "info", "time": "2023-11-15T13:02:53.259242", "tag": "s3 downloading", "message": "Got 10000 records from 2021/01/30/2021-01-30.csv"}
INFO:src.logger:{"level": "info", "time": "2023-11-15T13:02:56.444524", "tag": "data processing", "message": "Input data :2021/01/30/2021-01-30.csv processed raw input"}
INFO:src.logger:{"level": "info", "time": "2023-11-15T13:03:21.481832", "tag": "data processing", "message": "Got 2014 records for instance: daily_agg_by_hour"}
INFO:src.logger:{"level": "info", "time": "2023-11-15T13:03:21.482157", "tag": "s3 configs", "message": "Got next output path results/2021/01/30/daily_agg_20210130_SAA.csv in bucket - bucket_name"}