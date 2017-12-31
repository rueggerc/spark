package com.rueggerllc.spark.apps;

import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.rueggerllc.spark.beans.AveragePriceBean;

import scala.Tuple2;


// Data Format
// 0:UniqueID
// 1:Location
// 2:Price
// 3:Number of Bedrooms
// 4:Number of Bathrooms
// 5:Square Feet
// 6:Price Per Square Foot
// 7:State of Sale
//
// Sample:
// 132842,Arroyo Grande,795000.00,3,3,2371,335.30,Short Sale
//
// Problem:
// Output the average price for houses with different number of Bedrooms
// Average Price for 3 Bedroom House
// Average Price for 2 Bedroom House
// Average Price for 1 Bedroom House

public class AverageHousePrice {
	
	private static final Logger logger = Logger.getLogger(AverageHousePrice.class);
	public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);

        try {
        	logger.info("==== AverageHousePrice BEGIN ====");
        	
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("averageHousePrice");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        // Get input Data
	        JavaRDD<String> lines = sc.textFile("hdfs://captain:9000/inputs/RealEstate.csv");

	        // Filter out Header
	        // JavaRDD<String> lines = sc.textFile("input/RealEstate.csv");
	        lines = lines.filter(line -> filterRealEstateLines(line));
	        
	        // Create Mapping:
	        // # Bedrooms -> (1, price)
	        JavaPairRDD<Integer, AveragePriceBean> averagePriceBeans = lines.mapToPair(
	        		line -> new Tuple2<>(getNumberOfBedrooms(line), new AveragePriceBean(1, getPrice(line)))
	        );
	        
	        // Reduce By Number of Bedrooms
	        // Sum up Average Price Beans for each key
	        // # Bedrooms -> (totalNumberOfHomesWithNBedrooms, TotalPriceOfHomesWithNBedrooms)
	        JavaPairRDD<Integer, AveragePriceBean> averagePriceTotals = 
	        		averagePriceBeans.reduceByKey( (x,y) -> new AveragePriceBean(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));
	     
	        
	        // Compute Average Price for each entry
	        JavaPairRDD<Integer,String> averagePrices = 
	        		averagePriceTotals.mapValues(averagePriceBean -> String.valueOf(averagePriceBean.computeAverage()));
	           
	        // Diagnostics
//	        averagePrices.foreach(entry -> {
//	        	logger.info("Bedrooms=" + entry._1() + " AveragePrice=" + entry._2);
//	        });	
	        
	        averagePrices.saveAsTextFile("hdfs://captain:9000/outputs/RealEstate");
	        
	        if (sc != null) {
	        	sc.close();
	        }        	
        	logger.info("==== AverageHousePrice END ====");
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
	private static Integer getNumberOfBedrooms(String entry) {
		String[] splits = entry.split(COMMA_DELIMITER);
		Integer bedrooms = Integer.valueOf(splits[3]);
		return bedrooms;
	}
	private static Double getPrice(String entry) {
		String[] splits = entry.split(COMMA_DELIMITER);
		return Double.valueOf(splits[2]);
	}
	
	private static boolean filterRealEstateLines(String line) {
		if (line.trim().equals("")) {
			return false;
		}
		return !(line.split(COMMA_DELIMITER)[0].equals("MLS"));
	}
}