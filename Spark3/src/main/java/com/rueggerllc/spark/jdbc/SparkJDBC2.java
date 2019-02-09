package com.rueggerllc.spark.jdbc;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import com.rueggerllc.spark.beans.ReadingBean;

public class SparkJDBC2 {
	
	private static final Logger logger = Logger.getLogger(SparkJDBC2.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	logger.info("==== SparkJDBC2 BEGIN ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
       
              
	    // Get Our Session
        // -Dspark.master=local[*]
	    SparkSession spark = SparkSession
		    .builder()
		    .appName("SparkJDBC2")
		    .getOrCreate();
	    
	    // Dataset from JSON File
	    Dataset<Row> dataFrame1 =  getSourceFromJSON(spark);
	    
	    // Dataset from List
	    Dataset<Row> dataFrame2 =  getSourceFromList(spark);

	    // Dataset from CSV
	    Dataset<Row> dataFrame3 =  getSourceFromCSV(spark);
	   	  	     
	    // Write to Table
	    writeToSink(dataFrame1);
	    writeToSink(dataFrame2);
	    writeToSink(dataFrame3);
        
	   logger.info("==== SparkJDBC2 END ====");
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
    
    
    private static Dataset<Row> getSourceFromJSON(SparkSession spark) {
    	Encoder<ReadingBean> readingEncoder = Encoders.bean(ReadingBean.class);
	    String jsonPath = "input/reading.json";
	    Dataset<ReadingBean> readings = spark.read().json(jsonPath).as(readingEncoder);  
    	Dataset<Row>  dataFrame = readings.withColumn("reading_time", to_timestamp(col("reading_time").$div(1000L)));
    	
	    // Diagnostics and Sink
	    dataFrame.printSchema();
	    dataFrame.show();
    	return dataFrame;
    }
    
    private static Dataset<Row> getSourceFromCSV(SparkSession spark) {
	    String csvPath = "input/reading.csv";
	    DataFrameReader dataFrameReader = spark.read();
	    Dataset<Row> dataFrame = 
	        dataFrameReader
	        .format("org.apache.spark.csv")
	        .option("header","true")
	        .option("inferSchema", true)
	        .csv(csvPath);
    	dataFrame = dataFrame.withColumn("reading_time", to_timestamp(col("reading_time").$div(1000L)));
    	
	    // Diagnostics and Sink
	    dataFrame.printSchema();
	    dataFrame.show();
    	return dataFrame;
    }
    
    private static Dataset<Row> getSourceFromList(SparkSession spark) {
	    List<ReadingBean> readingBeanList = new ArrayList<>();
	    for (int i = 0; i < 3; i++) {
	    	ReadingBean reading = new ReadingBean();
	    	readingBeanList.add(reading);
	    	reading.setSensor_id("sensor1");
	    	reading.setNotes("Notes from List " + i);
	    	reading.setHumidity(42.33 + i);
	    	reading.setTemperature(78.64+i);
	    	long timestampLong = System.currentTimeMillis();
	    	reading.setReading_time(new Timestamp(timestampLong));  	
	    }
	    Dataset<ReadingBean> readings = spark.createDataset(readingBeanList, Encoders.bean(ReadingBean.class));
	    Dataset<Row> dataFrame = readings.toDF();
	    
	    // Diagnostics and Sink
	    dataFrame.printSchema();
	    dataFrame.show();
	    return dataFrame;
    }
    
    
    
    
}