package com.rueggerllc.spark.batch.sql;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CapitalOneToDB {
	
	private static final Logger logger = Logger.getLogger(CapitalOneToDB.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	String appName = CapitalOneToDB.class.getName();
    	logger.info(appName + " BEGIN");
        Logger.getLogger("org").setLevel(Level.ERROR);
       
              
	    // Get Our Session
        // -Dspark.master=local[*]
	    SparkSession spark = SparkSession
		    .builder()
		    .appName(appName)
		    .getOrCreate();
	    
	    // Get Source(s)
	    Dataset<Row> dataFrame =  getSourceFromCSV(spark);
	   	  	     
	    // Write to Sink(s)
	    writeToSink(dataFrame);
        
	    logger.info(appName + " END");
	   
    }
    
    private static Dataset<Row> getSourceFromCSV(SparkSession spark) {
    	// String csvPath = "hdfs://hp1:9000/user/Chris/input/capitalone";
	    String csvPath = "input/capitalone_2019_05.csv";
	    DataFrameReader dataFrameReader = spark.read();
	    Dataset<Row> dataFrame = 
	        dataFrameReader
	        .format("org.apache.spark.csv")
	        .option("header","true")
	        .option("inferSchema", true)
	        .csv(csvPath);
    	dataFrame.printSchema();
    	return dataFrame;
    }
    
    private static void writeToSink(Dataset<Row> dataFrame) {
	    String url = "jdbc:postgresql://localhost:5432/rueggerllc";
	    String table = "capitalone";
	    Properties connectionProperties = new Properties();
	    connectionProperties.setProperty("user", "chris");
	    connectionProperties.setProperty("password", "dakota");
	    connectionProperties.setProperty("Driver", "org.postgresql.Driver");
	    dataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties); 
	    
	    // Diagnostics and Sink
	    // dataFrame.show();
    }
    

}