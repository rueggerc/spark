package com.rueggerllc.spark.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import scala.Tuple2;

public class PairTests {

	private static Logger logger = Logger.getLogger(PairTests.class);

	private static String WS = "\\s+";
	
	// Notes

	
	
	@BeforeClass
	public static void setupClass() throws Exception {
	}
	
	@AfterClass
	public static void tearDownClass() throws Exception {
	}

	@Before
	public void setupTest() throws Exception {
	}

	@After
	public void tearDownTest() throws Exception {
	}
	
	@Test
	// @Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	

	
	@Test
	@Ignore
	public void testPairRDD1() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== PAIR RDD TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("testPairRDD1").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        List<Tuple2<String,Integer>> namesAndAges = new ArrayList<Tuple2<String,Integer>>();
	        namesAndAges.add(new Tuple2<>("Data", 12));
	        namesAndAges.add(new Tuple2<>("Fred", 43));
	        namesAndAges.add(new Tuple2<>("Barney", 39));
	        namesAndAges.add(new Tuple2<>("Wilma", 37));
	        JavaPairRDD<String, Integer> nameAgeRDD = sc.parallelizePairs(namesAndAges);
	        nameAgeRDD.foreach(entry -> {
	        	logger.info("Name=" + entry._1() + " Age=" + entry._2);
	        });
	        
	        nameAgeRDD.coalesce(1).saveAsTextFile("output/tuples.out");
	        

//	        List<Tuple2<String, Integer>> tuples = Arrays.asList(new Tuple2<>("Lily", 23),
//                    new Tuple2<>("Jack", 29),
//                    new Tuple2<>("Mary", 29),
//                    new Tuple2<>("James",8));
//
//	        JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(tuples);
//		    pairRDD.foreach(data -> {
//		        logger.info("name="+data._1() + " age=" + data._2());
//		    }); 	        

			logger.info("=== PAIR RDD TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testConvertToPairRDD() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== CONVERT TO PAIR RDD TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("testPairRDD1").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        List<String> inputStrings = Arrays.asList("Jack 12", "Foo 19", "Bar 36", "Captain 9");
	        JavaRDD<String> regularRDD = sc.parallelize(inputStrings);
	        
	        // Method 1
	        JavaPairRDD pairRDD1 = regularRDD.mapToPair(getNameAndAgePair());
	        pairRDD1.coalesce(1).saveAsTextFile("output/pairRDD1.out");
	       
	        // Method 2
	        JavaPairRDD<String,Integer> pairRDD2 = regularRDD.mapToPair(s->new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1])));
	        pairRDD2.coalesce(1).saveAsTextFile("output/pairRDD2.out");
	        
	        // Method 3
	        JavaPairRDD<String,Integer> pairRDD3 = regularRDD.mapToPair(s->new Tuple2<>(getKey(s), getIntegerValue(s)));
	        pairRDD3.coalesce(1).saveAsTextFile("output/pairRDD3.out");
	        
	        
	        logger.info("=== CONVERT TO PAIR RDD TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	
	@Test
	@Ignore
	public void testTransformWithFilter() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== CONVERT TO PAIR RDD TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("testPairRDD1").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        List<String> inputStrings = Arrays.asList("Jack 12", "Foo 19", "Bar 36", "Captain 9", "Later 12");
	        JavaRDD<String> regularRDD = sc.parallelize(inputStrings);
	       
	        
	        // Get Pair RDD
	        JavaPairRDD<String,Integer> pairRDD = regularRDD.mapToPair(s->new Tuple2<>(getKey(s), getIntegerValue(s)));
	        
	        // Filter out entries where name is not Jack
	        JavaPairRDD<String,Integer> pairsNotJack = pairRDD.filter(entry -> {
	        	return !entry._1().equals("Jack");
	        });
	        
	        pairsNotJack.foreach(entry -> {
	        	logger.info("NextKey=" + entry._1());
	        });
	        
	        
	        logger.info("=== CONVERT TO PAIR RDD TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	// @Ignore
	public void testTransformPairValue() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== CONVERT TO PAIR RDD TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("testPairRDD1").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        List<String> inputStrings = Arrays.asList("Captain Canine", "Oscar Feline", "Darwin Avian", "Sunny Avian");
	        JavaRDD<String> regularRDD = sc.parallelize(inputStrings);
	       
	        JavaPairRDD<String,String> pairRDD = regularRDD.mapToPair(s->new Tuple2<>(getKey(s), getStringValue(s)));
	        
	        // Capitalize the Species Name
	        JavaPairRDD<String,String> pairRDDCaps = pairRDD.mapValues(species -> species.toUpperCase());
	        pairRDDCaps.foreach(entry -> {
	        	logger.info("NextValue=" + entry._2());
	        });
	        
	        logger.info("=== CONVERT TO PAIR RDD TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	// PairFunction<T, K, V>
	// T=type in input RDD
	// K=Key type in Tuple
	// V=Value Type in Tuple
	private static PairFunction<String,String,Integer> getNameAndAgePair() {
		return (PairFunction<String,String,Integer>) s -> new Tuple2<>(s.split(" ")[0], Integer.valueOf(s.split(" ")[1]));
	}
	
	private static String getKey(String entry) {
		String[] splits = entry.split(" ");
		return splits[0];
		
	}
	private static String getStringValue(String entry) {
		String[] splits = entry.split(" ");
		return splits[1];
	}
	private static Integer getIntegerValue(String entry) {
		String[] splits = entry.split(" ");
		return Integer.valueOf(splits[1]);
	}
	
	
	
}
