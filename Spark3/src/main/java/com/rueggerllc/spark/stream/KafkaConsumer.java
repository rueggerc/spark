package com.rueggerllc.spark.stream;

import java.sql.Timestamp;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.StreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import com.rueggerllc.spark.beans.ReadingBean;

import scala.Tuple2;

public class KafkaConsumer {
	
	private static final Logger logger = Logger.getLogger(KafkaConsumer.class);
	// private static final String BROKERS = "kube:9092";
	private static final String BROKERS = "localhost:9092";
	private static final String topicName = "readings";

    public static void main(String[] args) throws Exception {
        logger.info("==== NEW Spark Kafka Consumer ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
        try {
        	
            
    	    // Get Our Session
            // -Dspark.master=local[*]
    	    SparkSession spark = SparkSession
    		    .builder()
    		    .appName("KafkaConsumer")
    		    .getOrCreate();
    	    
    	    // StreamingContext streamingContext  = new StreamingContext(spark.sparkContext(), Durations.minutes(60));
    	    StreamingContext streamingContext  = new StreamingContext(spark.sparkContext(), Durations.seconds(10));
    	    JavaStreamingContext javaStreamingContext = new JavaStreamingContext(streamingContext);
    	    
    	    System.out.println("=== MAIN OBJECTS CREATED===");
    	    
            Set<String> topics = new HashSet<>(Arrays.asList(topicName));
            Map<String, Object> kafkaParams = new HashMap<>();
            kafkaParams.put("bootstrap.servers", BROKERS);
            kafkaParams.put("key.deserializer", StringDeserializer.class);
            kafkaParams.put("value.deserializer", StringDeserializer.class);
            // kafkaParams.put("group.id", "spark-kafka-sensors");
            // kafkaParams.put("group.id", "spark-kafka-session-test");
            kafkaParams.put("group.id", topicName);
            kafkaParams.put("auto.offset.reset", "latest");
            kafkaParams.put("enable.auto.commit", false);
            
        	JavaInputDStream<ConsumerRecord<String, String>> dStream =
        		KafkaUtils.createDirectStream(
        			javaStreamingContext,
        			LocationStrategies.PreferConsistent(),
        			ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams));
        	
        	// Diagnostic: Display Messages
        	// dStream.foreachRDD(new MyRDDFunction());
        	
        	// Map to Key:Message
        	JavaPairDStream<String,String> messageStream = dStream.mapToPair(new MyMapToPairFunction());
        	JavaDStream<ReadingBean> beanStream = dStream.map(new MyMapToBeanFunction());
        	
        	// Output to sinks
        	beanStream.foreachRDD(new MyBeanRDDFunction());
        	// messageStream.print();
        	beanStream.print();
        	
        	
        	// Dataset<Row> dataFrame2 =  getSourceFromList(spark);
        	// Dataset<Person> personDS =  sqlContext.createDataset(personRDD.rdd(), Encoders.bean(Person.class))
        	
        	
        	// Start the computation
        	streamingContext.start();
        	streamingContext.awaitTermination();
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    private static void writeToSink(Dataset<Row> dataFrame) {
	    // Write to JDBC Sink
	    String url = "jdbc:postgresql://localhost:5432/rueggerllc";
	    String table = "reading";
	    Properties connectionProperties = new Properties();
	    connectionProperties.setProperty("user", "chris");
	    connectionProperties.setProperty("password", "dakota");
	    dataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);    	
    }
    
    private static class MyBeanRDDFunction implements VoidFunction<JavaRDD<ReadingBean>> {
		@Override
		public void call(JavaRDD<ReadingBean> javaRdd) throws Exception {
		}
    }  
  
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
    
    // sensor1,sensor1,68.68,16.92,1549769249261
    private static class MyMapToBeanFunction implements Function<ConsumerRecord<String,String>,ReadingBean> {
		@Override
		public ReadingBean call(ConsumerRecord<String, String> kafkaMessage) throws Exception {
			ReadingBean bean = new ReadingBean();
			String messageValue = kafkaMessage.value();
			String[] tokens = messageValue.split(",");
			String sensorID = tokens[0];
			Double temperature = Double.parseDouble(tokens[1]);
			Double humidity = Double.parseDouble(tokens[2]);
			long timestamp = Long.parseLong(tokens[3]);
			bean.setSensor_id(sensorID);
			bean.setNotes("");
			bean.setTemperature(temperature);
			bean.setHumidity(humidity);
			// bean.setReading_time(new Timestamp(timestamp/1000L));
			bean.setReading_time(new Timestamp(timestamp));
			return bean;
		}
    }  
      
    // <InputType, OutputTupleType1, OutputTupleType2>
    private static class MyMapToPairFunction implements PairFunction<ConsumerRecord<String,String>,String,String> {
		@Override
		public Tuple2<String, String> call(ConsumerRecord<String, String> input) throws Exception {
			System.out.println(String.format("MAP FUNCTION Consumer Record=%s %d %d %s", input.topic(), input.partition(), input.offset(), input.value()));
			return new Tuple2<>(input.key(), input.value());			
		}

    }  
    
    
    
    
}