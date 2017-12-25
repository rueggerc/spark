package com.rueggerllc.spark.apps;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class MyExampleHDFS {
	
	private static final Logger logger = Logger.getLogger(MyExampleHDFS.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== MyExampleHDFS BEGIN Version 2.0 ====");
        
//	    SparkConf conf = new SparkConf().setAppName("MyExampleHDFS")
//		         .setMaster("spark://192.168.243.128:7077")
//		         .set("spark.akka.heartbeat.interval", "100")
//		         .set("spark.local.ip", "127.0.0.1");
	    
	    // SparkConf conf = new SparkConf().setAppName("MyExampleHDFS").setMaster("spark://192.168.243.128:7077");
        SparkConf conf = new SparkConf().setAppName("MyExampleHDFS");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	   
	    logger.info("master=" + conf.get("master"));
	    logger.info("spark.master=" + conf.get("spark.master"));
	    
	    
	    logger.info("Context Created");
        
	    JavaRDD<String> lines = sc.textFile("hdfs://192.168.243.128:9000/my_storage/word_count.text");
	    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

//        Map<String, Long> wordCountByValue = words.countByValue();
//        logger.info("CountByValue:");
//
//        for (Map.Entry<String, Long> entry : wordCountByValue.entrySet()) {
//            logger.info(entry.getKey() + " : " + entry.getValue());
//        }
	    
	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        
		counts.saveAsTextFile("hdfs://192.168.243.128:9000/my_output/");
		
        if (sc != null) {
        	sc.close();
        }
        
        logger.info("==== MyExampleHDFS END ====");
    }
}