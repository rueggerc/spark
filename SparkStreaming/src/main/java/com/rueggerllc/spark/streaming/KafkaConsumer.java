package com.rueggerllc.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import kafka.serializer.StringDecoder;

public class KafkaConsumer {
	
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";
	
    public static void main(String[] args) throws Exception {
    	
        Logger.getLogger("org").setLevel(Level.ERROR);
        logger.info("==== Spark Kafka Consumer ====");
        try {
        	
            String topics = "ticker-topic";

            // Create context with a 2 seconds batch interval
            SparkConf sparkConf = new SparkConf().setAppName("SparkStreamKafka");
            JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(2));

            Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
            Map<String, String> kafkaParams = new HashMap<>();
            kafkaParams.put("metadata.broker.list", BROKERS);
            System.out.println("Brokers: " + BROKERS);
            
            JavaPairInputDStream<String, String> messages =
              KafkaUtils.createDirectStream(jssc, 
            		                        String.class, 
            		                        String.class, 
            		                        StringDecoder.class, 
            		                        StringDecoder.class, 
            		                        kafkaParams, 
            		                        topicsSet);

            // Force Lazy Evaluation
            messages.print();

            // Start the computation
            jssc.start();
            jssc.awaitTermination();
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
        

    }
}