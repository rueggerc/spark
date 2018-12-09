package com.rueggerllc.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class PetAnalysis {


    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        System.out.println("================ SPARK SQL BEGIN ===============");
        
        SparkSession session = SparkSession.builder().appName("SalarySurvey").master("local[*]").getOrCreate();
        // SparkSession session = SparkSession.builder().appName("PetAnalysis").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        System.out.println("================ READING DATA ===============");
        // Uses Header to infer schema of DataSet instead of providing schema manually.
        // Simplifies things
        Dataset<Row> pets = 
        		dataFrameReader
        		.format("org.apache.spark.csv")
        		.option("header","true")
        		.option("inferSchema", true)
        		.csv("input/pets.csv");

        // All types start out as Strings in inferred schema
        System.out.println("=== Print out inferred schema ===");
        pets.printSchema();

        System.out.println("=== Print 20 records===");
        pets.show(20);

        // SELECT species, weight
        System.out.println("=== Species and Weight ===");
        pets.select(col("species"),  col("weight")).show();

        // SELECT * 
        // FROM pets
        // WHERE species='canine'
        System.out.println("=== Display Canines ===");
        pets.filter(col("species").equalTo("canine")).show();

        // SELECT COUNT(*)
        // FROM pets
        // GROUP BY SPECIES
        System.out.println("=== Print the Count of Species ===");
        RelationalGroupedDataset groupedDataset = pets.groupBy(col("species"));
        groupedDataset.count().show();
        
        // Cast the Weight to an integer
        Dataset<Row> castedPets = pets.withColumn("weight", col("weight").cast("integer"));
        castedPets.printSchema();
        
        
        // SELECT INT(avg(weight)),max(weight)
        // GROUP BY species
        RelationalGroupedDataset groupedPets = castedPets.groupBy("species");
        groupedPets.agg(avg("weight").cast("integer"),max("weight")).show();
        
        
        session.stop();
    }
}
