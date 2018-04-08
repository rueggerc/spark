package com.rueggerllc.spark.sparkSQL;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.abs;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TransactionAnalysis {


    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        System.out.println("================ SPARK SQL BEGIN ===============");
        
        SparkSession session = SparkSession.builder().appName("TransactionAnalysis").master("local[*]").getOrCreate();
        // SparkSession session = SparkSession.builder().appName("TransactionAnalysis").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        // Uses Header to infer schema of DataSet instead of providing schema manually.
        // Simplifies things
        Dataset<Row> transactions = dataFrameReader.option("header","true").csv("hdfs://captain:9000/inputs/transactions.csv");
        
        getTotalForName(transactions, "SMILE TRAIN");
        getTotalForName(transactions, "SHRINERS");
        getTotalForName(transactions, "ST JUDE");
        getTotalForName(transactions, "PVA");
        getTotalForName(transactions, "ANTHEM");
        

        
        session.stop();
    }
    
    
    private static void getTotalForName(Dataset<Row> transactions, String name) {
  
        // SELECT Date, Name, abs(double(amount)) WHERE Name startsWith(name)
        System.out.println("Get TXNS For: " + name);
        Dataset<Row> selectedTXNs = transactions.filter(col("Name").startsWith(name)).withColumn("RealAmount", abs(col("amount").cast("double")));
        selectedTXNs = selectedTXNs.select(col("Date"), col("Name"), col("RealAmount"));
        selectedTXNs.show();
        
        // SELECT sum("RealAmount")
        selectedTXNs.agg(sum("RealAmount")).show();
        
        
        // SELECT sum(RealAmount)
        // RelationalGroupedDataset txnsByName = smileTrainTXNs.groupBy("Name");
        // txnsByName.agg(sum("RealAmount")).show();    	
    }
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
}
