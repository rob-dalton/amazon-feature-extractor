#!/bin/bash

# run with:
# > screen spark_submit.sh <script>.py

export SPARK_HOME=/usr/lib/spark
export PYTHONPATH=${SPARK_HOME}/python:$PYTHONPATH
export PYSPARK_PYTHON=$HOME/anaconda/bin/python

${SPARK_HOME}/bin/spark-submit \
    --master yarn \
        --deploy-mode client \
    --executor-memory 10G \
        --executor-cores 4 \
    --driver-memory 10G \
        --driver-cores 4 \
    --packages com.databricks:spark-csv_2.11:1.5.0 \
    --packages com.amazonaws:aws-java-sdk-pom:1.10.34 \
    --packages org.apache.hadoop:hadoop-aws:2.7.3 \
$1
