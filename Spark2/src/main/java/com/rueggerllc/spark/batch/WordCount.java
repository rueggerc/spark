package com.rueggerllc.spark.batch;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class WordCount {
	
	private static final Logger logger = Logger.getLogger(WordCount.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== WordCount BEGIN ====");
        
        // Get Spark Context
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
	    JavaSparkContext sc = new JavaSparkContext(conf);	   
	    logger.info("Context Created");
        
	    // Get Data
	    JavaRDD<String> lines = sc.textFile("input/rawlines.txt");
	    
	    // Transformations
	    JavaPairRDD<String,Integer> wordCount = lines
	    	.flatMap(new MyFlatMapper())
	    	.mapToPair(new MyMapFunction())
	    	.reduceByKey(new MyReduceFunction());
	    
	    // Write To Sink(s)
	    for (Tuple2<String,Integer> next : wordCount.collect()) {
	    	System.out.println("NEXT=" + next._1() + " " + next._2());
	    }
		wordCount.saveAsTextFile("output/wordCount");
		
	    // Done
        if (sc != null) {
        	sc.close();
        }
        
        logger.info("==== WordCount END ====");
    }
    
    private static class MyFlatMapper implements FlatMapFunction<String,String> {
		@Override
		public Iterator<String> call(String line) throws Exception {
			String[] tokenArray = line.toLowerCase().split(" ");
			List<String> tokens = Arrays.asList(tokenArray);
			return tokens.iterator();
		}
    }
    
    private static class MyMapFunction implements PairFunction<String,String,Integer> {
		@Override
		public Tuple2<String, Integer> call(String word) throws Exception {
			return new Tuple2<String,Integer>(word,new Integer(1));
		}
    }
    
    private static class MyReduceFunction implements Function2<Integer,Integer,Integer> {
		@Override
		public Integer call(Integer value1, Integer value2) throws Exception {
			return value1 + value2;
		}
    }
    
    
    
}