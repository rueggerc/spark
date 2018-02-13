package com.rueggerllc.spark.streaming;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class StreamingWordCount {
	
	private static final Logger logger = Logger.getLogger(StreamingWordCount.class);
	private static final Pattern SPACE = Pattern.compile(" ");

	
	// On Linux:
	// nc -lk 9999
	// -l 	listen
	// -k 	keep open
	
    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== StreamingWordCount BEGIN ====");
        
        SparkConf conf = new SparkConf().setAppName("StreamingWordCount");
	    JavaSparkContext sc = new JavaSparkContext(conf);
	   

	    if (args.length < 2) {
	    	System.err.println("Usage: StreamingWordCount <hostname> <port>");
	    	logger.error("Usage: StreamingWordCount <hostname> <port>");
	    	System.exit(1);
	    }
	    
	    String host = args[0];
	    String port = args[1];
	    logger.info("host=" + host);
	    logger.info("port=" + port);


	    // Create the context with a 1 second batch size
	    SparkConf sparkConf = new SparkConf().setAppName("StreamingWorCount");
	    sparkConf.set("spark.driver.allowMultipleContexts","true");
	    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, new Duration(5000));

	    // DStream: 
	    // Create a JavaReceiverInputDStream on target ip:port and count the
	    // words in input stream of \n delimited text (eg. generated by 'nc')
	    // Note that no duplication in storage level only for running locally.
	    // Replication necessary in distributed scenario for fault tolerance.
	    JavaReceiverInputDStream<String> lines = 
	    		ssc.socketTextStream(host, Integer.parseInt(port), StorageLevels.MEMORY_AND_DISK_SER);
	    JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
	    JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1)).reduceByKey((i1, i2) -> i1 + i2);

	    // wordCounts.foreachRDD(foreachFunc);
	    
	    // Display Output
	    wordCounts.print();
	    
	    // Start Program
	    ssc.start();
	    ssc.awaitTermination();
	    
	    if (ssc != null) {
	    	ssc.close();
	    }
        
        logger.info("==== StreamingWordCount END ====");
    }
}