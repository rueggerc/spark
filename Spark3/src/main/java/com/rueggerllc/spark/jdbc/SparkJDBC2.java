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

import com.rueggerllc.spark.beans.DummyBean;

public class SparkJDBC2 {
	
	private static final Logger logger = Logger.getLogger(SparkJDBC2.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	logger.info("==== SparkJDBC2 BEGIN ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
              
	    // Get Our Session
	    SparkSession spark = SparkSession
		    	.builder()
		    	.appName("SparkJDBC2")
		    	.master("local[*]")
		    	.getOrCreate();
	    
	    // Java Bean used to apply schema to JSON Data
	    Encoder<DummyBean> dummyEncoder = Encoders.bean(DummyBean.class);
	    
	    // Read JSON file to DataSet
	    String jsonPath = "input/dummy.json";
	    Dataset<DummyBean> readings = spark.read().json(jsonPath).as(dummyEncoder);
	    
	    // Diagnostics and Sink
	    readings.schema();
	    readings.show();
	    
	    
	    // Write to JDBC Sink
	    String url = "jdbc:postgresql://captain:5432/rueggerllc";
	    String table = "dummy";
	    Properties connectionProperties = new Properties();
	    connectionProperties.setProperty("user", "chris");
	    connectionProperties.setProperty("password", "dakota");
	    readings.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties);
	    
        
	   logger.info("==== SparkJDBC2 END ====");
    }
    
    
    
}