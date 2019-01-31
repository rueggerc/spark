package com.rueggerllc.spark.jdbc;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkJDBC1 {
	
	private static final Logger logger = Logger.getLogger(SparkJDBC1.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	logger.info("==== SparkJDBC1 BEGIN ====");
        Logger.getLogger("org").setLevel(Level.ERROR);
              
	    // Get Our Session
	    SparkSession spark = SparkSession
		    	.builder()
		    	.appName("SparkJDBC1")
		    	.master("local[*]")
		    	.getOrCreate();
	      
	    spark.read()
	    	.format("jdbc")
	    	.option("url","jdbc:postgresql://captain:5432/rueggerllc")
	    	.option("dbtable","dht22_readings")
	    	.option("user","chris")
	    	.option("password", "dakota")
	    	.load()
	    	.createOrReplaceTempView("my_spark_table");
	   
	    // Get Data Frame
	    Dataset<Row> rows = spark.sql("select * from my_spark_table");
	    rows.printSchema();
	    
	    // Write To Sink(s)
	   rows.show(20);
        
	   logger.info("==== SparkJDBC1 END ====");
    }
    
    
    
}