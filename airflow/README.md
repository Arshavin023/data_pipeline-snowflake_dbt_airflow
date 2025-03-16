# data pipeline for processing product orders and transactions 
## Overview
This repository contains procedures implementing dbt-core on Snowflake analytics on customer orders.

# Table of Contents
- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Execution Flow](#execution-flow)


## Introduction <a name="introduction"></a>
This repository contains SQL queries implemented with dbt-core for processing raw orders and transactions on Snowflake. Additionally, it contains Airflow DAG for workflow orchestration and scheduling.

## Prerequisites <a name="prerequisites"></a>
Before running this streaming data pipeline, the following prerequisite must be meant.

- Create a Snowflake Account, Snowflake User, Snowflake Data Warehouse, Snowflake Database and Snowflake Schema
- Connect DBT and Snowflake
- Virtual environment in directory with project directory

## Installation <a name="installation"></a>

- Install Airflow as a service

- Create Project directory
``` 
mkdir data_pipeline_snowflake_dbt_airflow
cd data_pipeline_snowflake_dbt_airflow
```

Clone the repository to your local machine:
``` 
git clone https://github.com/Arshavin023/data_pipeline-snowflake_dbt_airflow.git .
```

Move dbt_project into Airflow directory
```
mkdir /home/ubuntu/airflow/dbt_pipeline
cp -R /home/ubuntu/dbt_pipeline/* /home/ubuntu/airflow/dbt_pipeline/*
```

Install the required Python packages:
```
cd /home/ubuntu/airflow
pip install -r requirements.txt
```

Test DBT project
``` 
dbt run
dbt build
```