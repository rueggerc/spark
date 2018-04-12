#!/bin/bash
MASTER=spark://captain:7077
JAR_FILE=/home/hadoop/code/spark/MySpark/build/libs/MySpark.jar

# Remove output
hdfs dfs -rm -r /outputs/WordCount

# Submit
spark-submit --class com.rueggerllc.spark.apps.WordCount --master $MASTER $JAR_FILE
