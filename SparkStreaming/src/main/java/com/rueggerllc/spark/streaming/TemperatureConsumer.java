package com.rueggerllc.spark.streaming;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class TemperatureConsumer {
	
	private static final Logger logger = Logger.getLogger(TemperatureConsumer.class);
	
	private static final String BROKERS = "captain:9092,godzilla:9092,darwin:9092,oscar:9092";
	
    public static void main(String[] args) throws Exception {
    	executeNew();
    }
    

    
    private static void executeNew() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        logger.info("==== Temperature Consumer BEGIN ====");
        try {
        	
            SparkConf sparkConf = new SparkConf().setAppName("SparkStreamTemperatureConsumer");
            sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true");  
            JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(2));
            
            String topicName = "temperature-topic";
            Set<String> topics = new HashSet<>(Arrays.asList(topicName));
           
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", BROKERS);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            kafkaParams.put("group.id", "spark-temperature-consumer");
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);
            
        	
        	JavaInputDStream<ConsumerRecord<String, String>> dStream =
        			  KafkaUtils.createDirectStream(
        			    streamingContext,
        			    LocationStrategies.PreferConsistent(),
        			    ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
        			  );
        	
        	
        	dStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
        		  @Override
        		  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
        			  
//        	        rdd.foreach(entry -> {
//        	        	System.out.printf("offset = %d, key = %s, value = %s\n", entry.offset(), entry.key(), entry.value());
//        	        });	
        	        
        	        JavaRDD<String> messageRDD = rdd.map(entry -> entry.value() + " " + entry.timestamp() );
        	        if (messageRDD.count() > 0) {
        	        	System.out.println("GOT TEMPERATURE DATA!");
        	        	messageRDD.foreach(entry -> System.out.println(entry));
        	        	messageRDD.saveAsTextFile("output/spark/temperatures");
        	        } else {
        	        	System.out.println("EMPTY RDD");
        	        }
        			
//        		    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//        		    rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
//        		      @Override
//        		      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
//        		        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//        		        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//        		      }
//        		    });
        		  }
        		});
        	
        	
//        	
//        	JavaPairDStream<String,String> foo =
//        	dStream.mapToPair(
//        			  new PairFunction<ConsumerRecord<String, String>, String, String>() {
//        			    @Override
//        			    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
//        			      System.out.println("Mapping Consumer Record");
//        			      return new Tuple2<>(record.key(), record.value());
//        			    }
//        			  });
        	
        	// Start the computation
        	streamingContext.start();
        	streamingContext.awaitTermination();
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    
    
    
    
    
}