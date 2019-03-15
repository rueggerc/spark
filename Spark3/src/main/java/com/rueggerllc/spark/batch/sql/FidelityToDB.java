package com.rueggerllc.spark.batch.sql;

import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FidelityToDB {
	
	private static final Logger logger = Logger.getLogger(FidelityToDB.class);

    public static void main(String[] args) throws Exception {
    	
    	// Setup
    	String appName = FidelityToDB.class.getName();
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
    
    // Original
    // 12/31/2018,DEBIT,ROYALE WITH CHEESE 724-28318,24000978364666403396802; 05812;,-62.48
    // https://stackoverflow.com/questions/43259485/how-to-load-csvs-with-timestamps-in-custom-format
    private static Dataset<Row> getSourceFromCSV(SparkSession spark) {
    	
    	// Header:
    	// Date,Transaction,Name,Memo,Amount
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("Date",  DataTypes.TimestampType, true),
                DataTypes.createStructField("Transaction", DataTypes.StringType, true),
                DataTypes.createStructField("Name", DataTypes.StringType, true),
                DataTypes.createStructField("Memo", DataTypes.StringType, true),
                DataTypes.createStructField("Amount", DataTypes.DoubleType, true)
        });
    	
    	// String csvPath = "input/fidelityDate2.csv";
    	String csvPath = "hdfs://hp1:9000/user/Chris/input/fidelity";
	    DataFrameReader dataFrameReader = spark.read();
	    Dataset<Row> dataFrame = 
	        dataFrameReader
	        .format("org.apache.spark.csv")
	        .option("header","true")
	        .option("timestampFormat", "MM/dd/yyyy")
	        // .option("dateFormat", "MM/dd/yyyy")
	        .option("inferSchema", true)
	        .csv(csvPath);
    	
    	// Rename Columns
    	dataFrame = dataFrame.toDF("transaction_date","debit_credit", "description", "memo", "amount");
    	dataFrame.printSchema();
    	dataFrame.show(2);
    	
    	return dataFrame;
    }
    

    
    private static void writeToSink(Dataset<Row> dataFrame) {
	    String url = "jdbc:postgresql://localhost:5432/rueggerllc";
	    String table = "fidelity";
	    Properties connectionProperties = new Properties();
	    connectionProperties.setProperty("user", "chris");
	    connectionProperties.setProperty("password", "dakota");
	    connectionProperties.setProperty("Driver", "org.postgresql.Driver");
	    dataFrame.write().mode(SaveMode.Append).jdbc(url, table, connectionProperties); 
	    
	    // Diagnostics and Sink
	    // dataFrame.show(20);
    }
    
    

}