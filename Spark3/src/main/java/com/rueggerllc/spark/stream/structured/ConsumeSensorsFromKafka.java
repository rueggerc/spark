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

import com.rueggerllc.spark.beans.ReadingBean;

public class ConsumeSensorsFromKafka {
	
	private static final Logger logger = Logger.getLogger(ConsumeSensorsFromKafka.class);
	private static final String BROKERS = "localhost:9092";
	private static final String TOPIC = "readings";
	private static final String STARTING_OFFSET = "latest";

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
        	    .option("startingOffsets", STARTING_OFFSET)
        	    .load();
    	    
    	    // root
    	    // |-- key: binary (nullable = true)
    	    // |-- value: binary (nullable = true)
    	    // |-- topic: string (nullable = true)
    	    // |-- partition: integer (nullable = true)
    	    // |-- offset: long (nullable = true)
    	    // |-- timestamp: timestamp (nullable = true)
    	    // |-- timestampType: integer (nullable = true)
    	    dataFrame.printSchema();
    	    
    	    Dataset<Row> keyValueStream = dataFrame.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
//    	    StreamingQuery query = keyValueStream 
//    	    	      .writeStream()
//    	    	      .format("console")
//    	    	      .start();
    	    
//    	    Dataset<Row> valueStream = dataFrame.selectExpr("CAST(value AS STRING)");
//    	    valueStream.printSchema();
//    	    StreamingQuery query = valueStream 
//  	    	      .writeStream()
//  	    	      .format("console")
//  	    	      .start();
    	    
    	    
    	    Dataset<ReadingBean> readingsStream = keyValueStream.map(new MyMapToBeanFunction(), Encoders.bean(ReadingBean.class));
    	    // readingsStream.printSchema(); 	    
    	    
    	    Dataset<Row> readingsRowStream = readingsStream.toDF();
    	    // readingsRowStream.printSchema();
    	    // writeToSink(readingsRowStream);
    	    
    	    
    	    
    	    
    	   
    	    
    	    // root
    	    // |-- humidity: double (nullable = true)
    	    // |-- notes: string (nullable = true)
    	    // |-- reading_time: timestamp (nullable = true)
    	    // |-- sensor_id: string (nullable = true)
    	    // |-- temperature: double (nullable = true)
    	    StreamingQuery query = readingsRowStream 
  	    	      .writeStream()
  	    	      .format("console")
  	    	      .start();
    	    
    	    // Write to Sink
    	    // readingsRowStream.writeStream().foreachBatch(function);
    	    StreamingQuery writeToSinkQuery = readingsRowStream.writeStream().foreachBatch(new PostgresSink()).start();
    	    
    	    
//    	    readingsRowStream.writeStream().foreachBatch(
//    	    		  new VoidFunction2<Dataset<Row>, Long> {
//    	    		    public void call(Dataset<Row> dataset, Long batchId) {
//    	    		      // Transform and write batchDF
//    	    		    }    
//    	    		  }
//    	    		).start();
  	   
    	    // Output to Sink
//    	    StreamingQuery query = dataFrame
//    	      .writeStream()
//    	      .format("console")
//    	      .start();
    	    
    	    // Wait for Termination
    	    query.awaitTermination();
    	   	    
        	
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
    private static class PostgresSink implements VoidFunction2<Dataset<Row>,Long> {
		@Override
		public void call(Dataset<Row> dataset, Long v2) throws Exception {
			System.out.println("WRITE TO SINK");
			dataset.show(10);
			writeToSink(dataset);
		}
    }
    
    
    // HP1, 9.91,25.63,1549824596590
    private static class MyMapToBeanFunction implements MapFunction<Row,ReadingBean> {

		@Override
		public ReadingBean call(Row row) throws Exception {
			System.out.println("=== MAP TO BEAN ===");
			
			
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
			
			return readingBean;
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
    
    
}