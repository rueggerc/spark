package com.rueggerllc.stream;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import scala.Tuple2;

public class KafkaConsumer {
	
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	private static final String BROKERS = "sunny:9092";
	private static final String topicName = "readings";

    public static void main(String[] args) throws Exception {
        logger.info("==== NEW Spark Kafka Consumer ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
        try {
        	
            SparkConf sparkConf = new SparkConf().setAppName("SparkStreamKafkaConsumer");
            JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(1));
            
            Logger rootLogger = Logger.getRootLogger();
            rootLogger.setLevel(Level.ERROR);
            
            Set<String> topics = new HashSet<>(Arrays.asList(topicName));
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", BROKERS);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "spark-kafka-sensors");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);
            
            
            System.out.println("============== GET KAFKA STREAM BEGIN==========");
        	
        	JavaInputDStream<ConsumerRecord<String, String>> dStream =
        			KafkaUtils.createDirectStream(
        			streamingContext,
        			LocationStrategies.PreferConsistent(),
        			ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        			);
        	
        	// Diagnostic: Display Messages
        	dStream.foreachRDD(new MyRDDFunction());
        	
        	// Map to Key:Message
        	JavaPairDStream<String,String> messageStream = dStream.mapToPair(new MyMapFunction());       	
        	
        	// Output to sinks
        	messageStream.print();

        	// Start the computation
        	streamingContext.start();
        	streamingContext.awaitTermination();
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    // 
    private static class MyRDDFunction implements VoidFunction<JavaRDD<ConsumerRecord<String,String>>> {
		@Override
		public void call(JavaRDD<ConsumerRecord<String, String>> rdd) throws Exception {
			rdd.foreach(new MyConsumerRecordFunction());
		}
    }  
    
    private static class MyConsumerRecordFunction implements VoidFunction<ConsumerRecord<String,String>> {
 		@Override
 		public void call(ConsumerRecord<String, String> input) throws Exception {
 			System.out.println(String.format("RDD FUNCTION Consumer Record=%s %d %d %s", input.topic(), input.partition(), input.offset(), input.value()));
 		}

     }  
      
    // <InputType, OutputTupleType1, OutputTupleType2>
    private static class MyMapFunction implements PairFunction<ConsumerRecord<String,String>,String,String> {
		@Override
		public Tuple2<String, String> call(ConsumerRecord<String, String> input) throws Exception {
			System.out.println(String.format("MAP FUNCTION Consumer Record=%s %d %d %s", input.topic(), input.partition(), input.offset(), input.value()));
			return new Tuple2<>(input.key(), input.value());			
		}

    }  
    
    
    
    
}