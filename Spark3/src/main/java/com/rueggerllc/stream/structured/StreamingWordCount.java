package com.rueggerllc.stream.structured;

import java.time.Duration;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
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
        
	    if (args.length < 2) {
	    	System.err.println("Usage: StreamingWordCount <hostname> <port>");
	    	logger.error("Usage: StreamingWordCount <hostname> <port>");
	    	System.exit(1);
	    }
	    
	    String host = args[0];
	    String port = args[1];
	    logger.info("host=" + host);
	    logger.info("port=" + port);
       

	    SparkSession spark = SparkSession
	    	.builder()
	    	.appName("StructuredWordCount")
	    	.getOrCreate();
	    
	    // Create DataFrame representing the stream of input lines from connection to localhost:9999
	    Dataset<Row> lines = spark
	      .readStream()
	      .format("socket")
	      .option("host", host)
	      .option("port", port)
	      .load();
	    
	    System.out.println("BP0");

	    // Split the lines into words
//	    Dataset<String> words = lines
//	      .as(Encoders.STRING())
//	      .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

	    // Split the lines into words
	    Dataset<String> words = lines
	      .as(Encoders.STRING())
	      .flatMap(new MyFlatMapper(), Encoders.STRING());

	    
	    // Generate running word count
	    Dataset<Row> wordCounts = words.groupBy("value").count();
	    
	    // Start running the query that prints the running counts to the console
	    StreamingQuery query = wordCounts.writeStream()
	      .outputMode("complete")
	      .format("console")
	      // .trigger(Trigger.ProcessingTime(10))
	      // .trigger(Trigger.Continuous(1000))
	      .start();

	    System.out.println("OK HERE WE GO");
	    
	    // Wait
	    query.awaitTermination();
	    
        logger.info("==== StreamingWordCount END ====");
    }
    
    private static class MyFlatMapper implements FlatMapFunction<String,String> {
		// @Override
		public Iterator<String> call(String line) throws Exception {
			String[] tokenArray = line.toLowerCase().split(" ");
			List<String> tokens = Arrays.asList(tokenArray);
			return tokens.iterator();
		}
    }   
    
    
    
    
    
    
    
    
    
    
    
}