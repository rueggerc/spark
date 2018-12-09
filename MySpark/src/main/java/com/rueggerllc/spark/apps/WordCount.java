package com.rueggerllc.spark.apps;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WordCount {
	
	private static final Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== WordCount BEGIN ====");
        
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
        // SparkConf conf = new SparkConf().setAppName("WordCount");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	   
	    // logger.info("master=" + conf.get("master"));
	    // logger.info("spark.master=" + conf.get("spark.master"));
	    logger.info("Context Created");
        
	    // JavaRDD<String> lines = sc.textFile("hdfs://captain:9000/inputs/word_count.text");
	    JavaRDD<String> lines = sc.textFile("hdfs://captain:9000/inputs/constitution.txt");
	    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
	    
	    JavaPairRDD<String, Integer> ones = words.mapToPair(s -> new Tuple2<>(s, 1));
	    JavaPairRDD<String, Integer> counts = ones.reduceByKey((i1, i2) -> i1 + i2);
        
		counts.saveAsTextFile("hdfs://captain:9000/outputs/WordCount/");
		
        if (sc != null) {
        	sc.close();
        }
        
        logger.info("==== WordCount END ====");
    }
}