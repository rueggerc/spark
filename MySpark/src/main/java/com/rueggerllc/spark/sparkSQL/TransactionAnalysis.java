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
        // SparkSession session = SparkSession.builder().appName("PetAnalysis").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        // Uses Header to infer schema of DataSet instead of providing schema manually.
        // Simplifies things
        Dataset<Row> transactions = dataFrameReader.option("header","true").csv("hdfs://captain:9000/inputs/transactions.csv");
        
        getTotalForName(transactions, "SMILE TRAIN");
        getTotalForName(transactions, "SHRINERS");
        getTotalForName(transactions, "ST JUDE");
        getTotalForName(transactions, "PVA");
        getTotalForName(transactions, "ANTHEM");
        

 /*       // All types start out as Strings in inferred schema
        System.out.println("=== Inferred Schema BEGIN ===");
        transactions.printSchema();
        System.out.println("=== Inferred Schema END ===");

        // System.out.println("=== First 20 records===");
        // transactions.show(20);
        // System.out.println("=== First 20 records END ===");
        
        // SELECT Date, Name, abs(double(amount)) WHERE Name startsWith('SMILE TRAIN')
        System.out.println("=== Get SMILE TRAIN ===");
        Dataset<Row> smileTrainTXNs = transactions.filter(col("Name").startsWith("SMILE TRAIN")).withColumn("RealAmount", abs(col("amount").cast("double")));
        // smileTrainTXNs = smileTrainTXNs.select(col("Date"), col("Name"), abs(col("RealAmount")));
        smileTrainTXNs = smileTrainTXNs.select(col("Date"), col("Name"), col("RealAmount"));
        smileTrainTXNs.show();
        
        
        // smileTrainTXNs = smileTrainTXNs.withColumn("RealAmount", col("amount").cast("double"));
        // smileTrainTXNs = smileTrainTXNs.withColumn("RealAmount", abs());
        
        
        // SELECT sum("RealAmount")
        smileTrainTXNs.agg(sum("RealAmount")).show();
        
        
        // SELECT sum(RealAmount)
        // RelationalGroupedDataset txnsByName = smileTrainTXNs.groupBy("Name");
        // txnsByName.agg(sum("RealAmount")).show();
*/        
        
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
