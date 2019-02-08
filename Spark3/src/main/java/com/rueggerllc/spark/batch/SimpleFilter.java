package com.rueggerllc.spark.batch;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class SimpleFilter {
	
	private static final Logger logger = Logger.getLogger(SimpleFilter.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== FilterEvenNumbers BEGIN ====");
        
        // Get Spark Context
        SparkConf conf = new SparkConf().setAppName("FilterEvenNumbers").setMaster("local[*]");
	    JavaSparkContext sc = new JavaSparkContext(conf);	   
	    logger.info("Context Created");
        
	    // Get Data
	    JavaRDD<String> lines = sc.textFile("input/numbers.txt");
	    JavaRDD<String> evenLines = lines
	    	.filter(new EvenNumberFilter());
	    
	    // Write To Sink(s)
	    for (String next : evenLines.collect()) {
	    	System.out.println("NEXT EVEN NUMBER=" + next);
	    }
	    
	    // Done
        if (sc != null) {
        	sc.close();
        }
        logger.info("==== FilterEvenNumbers END ====");
    }
    
    
    private static class EvenNumberFilter implements Function<String,Boolean> {
		@Override
		public Boolean call(String line) throws Exception {
			Integer integerValue = Integer.parseInt(line);
			if ((integerValue % 2) == 0) {
				return true;
			}
			return false;
		}
    }
    
    
}