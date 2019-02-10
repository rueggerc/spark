package com.rueggerllc.stream.structured;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class ConsumeSensorsFromKafka {
	
	private static final Logger logger = Logger.getLogger(ConsumeSensorsFromKafka.class);
	private static final String BROKERS = "localhost:9092";
	private static final String TOPIC = "readings";

    public static void main(String[] args) throws Exception {
        logger.info("==== NEW Spark Kafka Consumer ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
        try {
        	
        	System.out.println(ConsumeSensorsFromKafka.class.getName());
        	
    	    // Get Our Session
            // -Dspark.master=local[*]
    	    SparkSession spark = SparkSession
    		    .builder()
    		    .appName(ConsumeSensorsFromKafka.class.getName())
    		    .getOrCreate();
    	    
    	    
    	    Dataset<Row> dataFrame = spark
        	    .readStream()
        	    .format("kafka")
        	    .option("kafka.bootstrap.servers", BROKERS)
        	    .option("subscribe", TOPIC)
        	    .option("startingOffsets", "latest")
        	    .load();
    	    
    	    dataFrame.printSchema();
    	   
    	    // Output to Sink
    	    StreamingQuery query = dataFrame
    	      .writeStream()
    	      .format("console")
    	      .start();
    	    
    	    // Wait for Termination
    	    query.awaitTermination();
    	   	    
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    
}