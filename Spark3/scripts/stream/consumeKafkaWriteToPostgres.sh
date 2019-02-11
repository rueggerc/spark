#!/bin/bash

MASTER=spark://captain:7077
JAR_FILE=/home/hadoop/code/spark/Spark3/target/Spark3-0.0.1-SNAPSHOT-jar-with-dependencies.jar
CLASS=com.rueggerllc.spark.stream.ConsumeKafkaWriteToPostgres

spark-submit --class $CLASS --master $MASTER $JAR_FILE
