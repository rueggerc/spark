package com.rueggerllc.spark.tests;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.spark.beans.Foo;
import com.rueggerllc.spark.beans.MyFilter;
import com.rueggerllc.spark.functions.MyPredicate;

public class CoreTests {

	private static Logger logger = Logger.getLogger(CoreTests.class);

	
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
	public void testLamda1() {
		logger.info("Test Lambda1");
		Predicate<Integer> isOdd = n -> n % 2 != 0;
	}
	
	@Test
	// @Ignore
	public void testFlatMap() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
			
			String[] petsArray = {"Captain Foobar the Later", "Darwin the", "Oscar hey Bud Lets Party"};
			List<String> thePets = Arrays.asList(petsArray);
			
		    SparkConf conf = new SparkConf().setAppName("myFlatMap").setMaster("local[2]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    
		    
		    
		    JavaRDD<String> lines = sc.parallelize(thePets);
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
		    logger.info("========= Test FlatMap BEGIN ===========");
		    
		    // Collect RDD for printing
	        for(String word : words.collect()){
	            logger.info(word);
	        }		    
	        logger.info("========= Test FlatMap END ===========");
	        
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	@Test
	@Ignore
	public void testSparkCentOS1() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
			SparkConf conf = new SparkConf().setAppName("myFlatMap").setMaster("local[2]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    JavaRDD<String> lines = sc.textFile("hdfs://192.168.243.128:9000/my_storage/word_count.text");
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	@Test
	@Ignore
	public void testSpark() {
		
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			
	
		    SparkConf conf = new SparkConf().setAppName("mySparkTest")
		    		         .setMaster("spark://192.168.243.123:7077")
		    		         .set("spark.akka.heartbeat.interval", "100")
		    		         .set("spark.local.ip", "127.0.0.1");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    JavaRDD<String> lines = sc.textFile("hdfs://192.168.243.123:9000/my_storage/word_count.text");
		    JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    
		    logger.info("========= Test FlatMap BEGIN ===========");
		    
		    // Collect RDD for printing
	        for(String word : words.collect()){
	            logger.info(word);
	        }		    
	        logger.info("========= Test FlatMap END ===========");
	        
	        Map<String, Long> wordCounts = words.countByValue();
	        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
	            logger.info(entry.getKey() + " : " + entry.getValue());
	        }
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
		

	}
	
	
	@Test
	@Ignore
	public void testFilterOdds() {
		logger.info("Test Filter Odds");
		Integer[] intsArray = {0,1,2,3,4,5,6,7,8,9};
		List<Integer> theIntegers = Arrays.asList(intsArray);
		Collection<Integer> theOdds = MyFilter.filter(n -> n % 2 != 0, theIntegers);
		for (Integer next : theOdds) {
			logger.info("Next Odd=" + next);
		}
		
		
	}
	
	
	@Test
	@Ignore
	public void testFilterEvens() {
		logger.info("Test Filter Evens");
		Integer[] intsArray = {0,1,2,3,4,5,6,7,8,9};
		List<Integer> theIntegers = Arrays.asList(intsArray);
		MyPredicate<Integer> myPredicate = (x) -> {return x % 2 == 0;};
		Collection<Integer> theOdds = MyFilter.filterWithMyPredicate(myPredicate, theIntegers);
		for (Integer next : theOdds) {
			logger.info("Next Odd=" + next);
		}
	}
	
	
	@Test
	@Ignore
	public void testFooFunction() {
		logger.info("Test Foo Function");
		String[] petsArray = {"Captain", "Darwin", "Oscar"};
		List<String> thePets = Arrays.asList(petsArray);
		Foo.runTheFunction((p) -> {return p + "_ChrisPet";}, thePets);
		
		Foo.runTheFunction((p) -> {return p + "_Later!";}, thePets);
	}
	
	
	
}
