package com.rueggerllc.spark.jdbc;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoder;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.to_utc_timestamp;
import com.rueggerllc.spark.beans.DummyBean;

public class SparkJDBC2 {
	
	private static final Logger logger = Logger.getLogger(SparkJDBC2.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	logger.info("==== SparkJDBC2 BEGIN ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        System.out.println("Time=" + System.currentTimeMillis());
              
	    // Get Our Session
        // -Dspark.master=local[*]
	    SparkSession spark = SparkSession
		    	.builder()
		    	.appName("SparkJDBC2")
		    	.getOrCreate();
	    
	    // Java Bean used to apply schema to JSON Data
	    Encoder<DummyBean> dummyEncoder = Encoders.bean(DummyBean.class);
	    
	    // Read JSON file to DataSet
	    String jsonPath = "input/dummy.json";
	    Dataset<DummyBean> readings = spark.read().json(jsonPath).as(dummyEncoder);
	    
	    // Diagnostics and Sink
	    readings.printSchema();
	    readings.show();
	    
	    // readings.withColumn("reading_time", to_utc_timestamp(col("reading_time").$div(1000L),"UTC")).show(false);
	    // readings.withColumn("ts", to_timestamp(col("reading_time/1000"))).show(false);
	    // Dataset<Row>  readingsRow = readings.withColumn("reading_time", to_timestamp(col("reading_time").$div(new Integer(1000))));
	    Dataset<Row>  readingsRow = readings.withColumn("reading_time", to_timestamp(col("reading_time").$div(1000L)));
	    // Dataset<Row>  readingsRow = readings.withColumn("reading_time", to_utc_timestamp(col("reading_time").$div(1000L),"UTC"));
	    
	    
	    // Write to JDBC Sink
	    String url = "jdbc:postgresql://localhost:5432/rueggerllc";
	    String table = "dummy";
	    Properties connectionProperties = new Properties();
	    connectionProperties.setProperty("user", "chris");
	    connectionProperties.setProperty("password", "dakota");
	    readingsRow.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);

        
	   logger.info("==== SparkJDBC2 END ====");
    }
    
    
    
}