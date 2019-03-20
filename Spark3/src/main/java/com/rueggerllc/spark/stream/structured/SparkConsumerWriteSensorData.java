package com.rueggerllc.spark.stream.structured;

import java.sql.Timestamp;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

import com.rueggerllc.spark.beans.ReadingBean;

import scala.concurrent.duration.Duration;

public class SparkConsumerWriteSensorData {
	
	private static final Logger logger = Logger.getLogger(SparkConsumerWriteSensorData.class);	
	private static final String BROKERS = "kube:9092";
	private static final String TOPIC = "sensors";
	private static final String STARTING_OFFSET = "latest";

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        try {
        	
        	System.out.println(SparkConsumerWriteSensorData.class.getName());
        	
    	    // Get Our Session
            // -Dspark.master=local[*]
    	    SparkSession spark = SparkSession
    		    .builder()
    		    .appName(SparkConsumerWriteSensorData.class.getName())
    		    .getOrCreate();
    	    
    	    Dataset<Row> dataFrame = spark
        	    .readStream()
        	    .format("kafka")
        	    .option("kafka.bootstrap.servers", BROKERS)
        	    .option("subscribe", TOPIC)
        	    .option("startingOffsets", STARTING_OFFSET)
        	    .load();
    	    
    	    // schema
    	    // |-- key: binary (nullable = true)
    	    // |-- value: binary (nullable = true)
    	    // |-- topic: string (nullable = true)
    	    // |-- partition: integer (nullable = true)
    	    // |-- offset: long (nullable = true)
    	    // |-- timestamp: timestamp (nullable = true)
    	    // |-- timestampType: integer (nullable = true)
    	    dataFrame.printSchema();
    	    
    	    // Get Kafka Message and Encode to Bean
    	    Dataset<Row> keyValueStream = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
    	    Dataset<ReadingBean> readingsStream = keyValueStream.map(new MyMapToBeanFunction(), Encoders.bean(ReadingBean.class));
    	    Dataset<Row> readingsRowStream = readingsStream.toDF();
    	    readingsRowStream.printSchema();
 
    	    // Write Bean Data to Sink(s)
    	    // root
    	    // |-- humidity: double (nullable = true)
    	    // |-- notes: string (nullable = true)
    	    // |-- reading_time: timestamp (nullable = true)
    	    // |-- sensor_id: string (nullable = true)
    	    // |-- temperature: double (nullable = true)
    	    StreamingQuery streamingQuery = readingsRowStream
    	    	.writeStream()
    	    	.trigger(Trigger.ProcessingTime("60 seconds"))
    	    	.format("console")
    	    	.foreachBatch(new PostgresSink())
    	    	.start();
    	    
    	    // Wait for Termination
    	    streamingQuery.awaitTermination();
    	   	    
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    // Write to JDBC Sink
    private static class PostgresSink implements VoidFunction2<Dataset<Row>,Long> {
		@Override
		public void call(Dataset<Row> dataFrame, Long v2) throws Exception {
			logger.info("Write Dataframe to to Postgres");
		   
		    String url = "jdbc:postgresql://captain:5432/rueggerllc";
		    String table = "spark_readings";
		    Properties connectionProperties = new Properties();
		    connectionProperties.setProperty("user", "chris");
		    connectionProperties.setProperty("password", "dakota");
		    connectionProperties.setProperty("Driver", "org.postgresql.Driver");
		    dataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);    	
		}
    }
    
    
    // HP1, 9.91,25.63,1549824596590
    private static class MyMapToBeanFunction implements MapFunction<Row,ReadingBean> {

		@Override
		public ReadingBean call(Row row) throws Exception {
			int index = row.fieldIndex("value");
			String message = row.getString(index);
			String[] tokens = message.split(",");
			String sensorID = tokens[0];
			double temperature = Double.parseDouble(tokens[1]);
			double humidity = Double.parseDouble(tokens[2]);
			long timestamp = Long.parseLong(tokens[3]);
			
			ReadingBean readingBean = new ReadingBean();
			readingBean.setSensor_id(sensorID);
			readingBean.setTemperature(temperature);
			readingBean.setNotes("Spark!");
			readingBean.setHumidity(humidity);
			readingBean.setReading_time(new Timestamp(timestamp));
			
			logger.info(String.format("Got Reading %s %.2f %.2f", sensorID, temperature, humidity));
			return readingBean;
		}
    }  
    
    
}