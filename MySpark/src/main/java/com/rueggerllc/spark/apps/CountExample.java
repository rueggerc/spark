package com.rueggerllc.spark.apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class CountExample {
	
	private static final Logger logger = Logger.getLogger(CountExample.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== CountExample BEGIN ====");
        
        // SparkConf conf = new SparkConf().setAppName("count").setMaster("local[*]");
        SparkConf conf = new SparkConf().setAppName("CountExample");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<String> inputWords = Arrays.asList("spark", "hadoop", "spark", "hive", "pig", "cassandra", "hadoop");
        JavaRDD<String> wordRdd = sc.parallelize(inputWords);

        logger.info("Count: " + wordRdd.count());

        Map<String, Long> wordCountByValue = wordRdd.countByValue();

        logger.info("CountByValue:");

        for (Map.Entry<String, Long> entry : wordCountByValue.entrySet()) {
            logger.info(entry.getKey() + " : " + entry.getValue());
        }
        logger.info("==== CountExample END ====");
    }
}