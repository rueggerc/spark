package com.rueggerllc.spark.apps;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;

import scala.Option;


// Data Format
// 0:UniqueID


public class StackOverflowSurvey {
	
	private static final Logger logger = Logger.getLogger(StackOverflowSurvey.class);
	public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";

    public static void main(String[] args) throws Exception {
        // Logger.getLogger("org").setLevel(Level.ERROR);

        try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey");

	        SparkContext sparkContext = new SparkContext(conf);
	        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

	        final LongAccumulator total = new LongAccumulator();
	        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();
	        final LongAccumulator totalFromCanada = new LongAccumulator();
	        final LongAccumulator totalBytesProcessed = new LongAccumulator();

	        total.register(sparkContext, Option.apply("total"), false);
	        totalFromCanada.register(sparkContext, Option.apply("total From Canada"), false);
	        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);
	        totalBytesProcessed.register(sparkContext, Option.apply("total Bytes Processed"), false);

	        JavaRDD<String> responseRDD = javaSparkContext.textFile("hdfs://captain:9000/inputs/2016-stack-overflow-survey-responses.csv");
	        JavaRDD<String> processedRDD = responseRDD.filter(response -> {
	            String[] splits = response.split(COMMA_DELIMITER, -1);

	            total.add(1);
	            
	            // Get total # of bytes
	            totalBytesProcessed.add(response.getBytes().length);

	            if (splits[14].isEmpty()) {
	                missingSalaryMidPoint.add(1);
	            }
	            
	            if (splits[2].equals("Canada")) {
	            	totalFromCanada.add(1);
	            }
	            
	            // Done
	            return true;

	        });

	        logger.info("Results:\n");
	        
	        // Necessary to force lazy evaluation
	        logger.info("Records Processed: " + processedRDD.count());
	        
	        logger.info("Total count of responses: " + total.value());
	        logger.info("Total From Canada: " + totalFromCanada.value());
	        logger.info("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
	        
        } catch (Exception e) {
        	logger.error("ERROR", e);
        }
    }
    
}