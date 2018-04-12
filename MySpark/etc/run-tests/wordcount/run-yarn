#!/bin/bash
MASTER=yarn
DEPLOY_MODE=cluster
JAR_FILE=/home/hadoop/code/spark/MySpark/build/libs/MySpark.jar

# Cleanup
hdfs dfs -rm -r /outputs/WordCount

# Submit
spark-submit --class com.rueggerllc.spark.apps.WordCount --master $MASTER --deploy-mode $DEPLOY_MODE $JAR_FILE
