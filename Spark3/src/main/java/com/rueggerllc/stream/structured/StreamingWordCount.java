package com.rueggerllc.stream.structured;

import java.time.Duration;
import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class StreamingWordCount {
	
	private static final Logger logger = Logger.getLogger(StreamingWordCount.class);
	private static final Pattern SPACE = Pattern.compile(" ");


    public static void main(String[] args) throws Exception {
        
        logger.info("==== Structured Streaming WordCount BEGIN ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
        

	    SparkSession spark = SparkSession
	    	.builder()
	    	.appName("StructuredWordCount")
	    	.getOrCreate();
	    
	    // Create DataFrame representing the stream of input lines from connection to localhost:9999
	    Dataset<Row> lines = spark
	      .readStream()
	      .format("socket")
	      .option("host", "captain")
	      .option("port", 9999)
	      .load();
	    
	    System.out.println("BP0");

	    // Split the lines into words
	    Dataset<String> words = lines
	      .as(Encoders.STRING())
	      .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

	    // Generate running word count
	    Dataset<Row> wordCounts = words.groupBy("value").count();
	    
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
	      .outputMode("complete")
	      .format("console")
	      // .trigger(Trigger.ProcessingTime(10))
	      // .trigger(Trigger.Continuous(1000))
	      .start();

	    System.out.println("YEAH DOG LETS GO");
	    
	    // Wait
	    query.awaitTermination();
	    
    
        logger.info("==== StreamingWordCount END ====");
    }
}