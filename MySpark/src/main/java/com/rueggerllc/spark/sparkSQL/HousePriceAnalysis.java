package com.rueggerllc.spark.sparkSQL;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.*;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;

public class HousePriceAnalysis {

  /* Create a Spark program to read the house data from in/RealEstate.csv,
     group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.

	 The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
	 around it. 
	
	 The dataset contains the following fields:
	 1. MLS: Multiple listing service number for the house (unique ID).
	 2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
	 northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
	 some out of area locations as well.
	 3. Price: the most recent listing price of the house (in dollars).
	 4. Bedrooms: number of bedrooms.
	 5. Bathrooms: number of bathrooms.
	 6. Size: size of the house in square feet.
	 7. Price/SQ.ft: price of the house per square foot.
	 8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.
	
	 Each field is comma separated.
	
	 Sample output:
	
	 +----------------+-----------------+----------+
	 |        Location| avg(Price SQ Ft)|max(Price)|
	 +----------------+-----------------+----------+
	 |          Oceano|           1145.0|   1195000|
	 |         Bradley|            606.0|   1600000|
	 | San Luis Obispo|            459.0|   2369000|
	 |      Santa Ynez|            391.4|   1395000|
	 |         Cayucos|            387.0|   1500000|
	 |.............................................|
	 |.............................................|
	 |.............................................|

  */
	
    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        System.out.println("================ SPARK SQL BEGIN ===============");
        
        SparkSession session = SparkSession.builder().appName("HousePriceAnalysis").master("local[*]").getOrCreate();
        // SparkSession session = SparkSession.builder().appName("PetAnalysis").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        System.out.println("=== READ DATA ====");
        Dataset<Row> listings = dataFrameReader.option("header","true").csv("input/RealEstate.csv");
        // listings.printSchema();
        // listings.show(20);

        // GOAL:
        // group by location, aggregate the average price per SQ Ft and max price, and sort by average price per SQ Ft.
        Dataset<Row> castedListings = listings.withColumn("price_sq_ft", col("Price SQ Ft").cast("long"))
        		                              .withColumn("price", col("Price").cast("long"));
        
        // SELECT avg(price_sq_ft), max(price)
        // GROUP BY Location
        // ORDER BY avg(price_sq_ft) DESCENDING
        Dataset<Row> groupedListings = 
          castedListings.groupBy(col("Location"))
                        .agg(avg("price_sq_ft"),max("price"))
                        .orderBy( col("avg(price_sq_ft)").desc() ); 
        groupedListings.show();
        
        session.stop();
    }
}
