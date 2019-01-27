package com.rueggerllc.stream;

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
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

import scala.Tuple2;

public class KafkaConsumer {
	
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	// private static final String BROKERS = "kube:9092";
	private static final String BROKERS = "localhost:9092";
	
    public static void main(String[] args) throws Exception {
    	executeNew();
    }
    
    
    private static void executeNew() {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
     
        
        logger.info("==== NEW Spark Kafka Consumer ====");
        try {
        	
            SparkConf sparkConf = new SparkConf().setAppName("SparkStreamKafkaConsumer");
            JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.minutes(1));
            
            Logger rootLogger = Logger.getRootLogger();
            rootLogger.setLevel(Level.ERROR);
            
            // String topicName = "dummy-topic";
            String topicName = "readings";
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
        	
//        	dStream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
//        		  @Override
//        		  public void call(JavaRDD<ConsumerRecord<String, String>> rdd) {
//        			  
//        			// System.out.println("NEXT RDD");
//        	        rdd.foreach(entry -> {
//        	        	System.out.printf("offset = %d, key = %s, value = %s\n", entry.offset(), entry.key(), entry.value());
//        	        });	
//        			
//        		    final OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
//        		    rdd.foreachPartition(new VoidFunction<Iterator<ConsumerRecord<String, String>>>() {
//        		      @Override
//        		      public void call(Iterator<ConsumerRecord<String, String>> consumerRecords) {
//        		        OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
//        		        System.out.println(o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
//        		      }
//        		    });
//        		  }
//        		});
        	
        	
        	
        	JavaPairDStream<String,String> foo =
        	dStream.mapToPair(
        			  new PairFunction<ConsumerRecord<String, String>, String, String>() {
        			    @Override
        			    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {
        			      System.out.println(String.format("Got Consumer Record=%s %d %d %s",
        			    		  record.topic(), record.partition(), record.offset(), record.value()));
        			      return new Tuple2<>(record.key(), record.value());
        			    }
        			  });
        	
        	
        	// Output to sinks
        	// dstream.map(record=>(record.value().toString)).print
        	// dStream.print(10);
//        	
        	foo.print();
//        	// dStream.print(10);
        	
        	// Start the computation
        	streamingContext.start();
        	streamingContext.awaitTermination();
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    
    
    
    
    
}