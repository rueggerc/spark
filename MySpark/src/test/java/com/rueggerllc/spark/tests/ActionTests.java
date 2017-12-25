package com.rueggerllc.spark.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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

public class ActionTests {

	private static Logger logger = Logger.getLogger(ActionTests.class);

	private static String WS = "\\s+";
	
	// Notes
	// collect used for unit test/debugging
	// DO NOT use on large RDD's - entire RDD must fit into memory of driver program!
	
	// count
	// countByValue
	
	
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
	public void testCollect() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== COLLECT TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("collectTest").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<String> thePetsList = Arrays.asList("Captain", "Dakota", "Dakota", "Darwin", "Sunny");
		    JavaRDD<String> petsRDD = sc.parallelize(thePetsList);
			
		    List<String> collectedPets = petsRDD.collect();
		    for (String pet : collectedPets) {
		    	logger.info("Next Pet=" + pet);
		    }

			logger.info("=== COLLECT TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testCountByValue() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== COUNT TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("countByValueTest").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<String> thePetsList = Arrays.asList("Captain", "Dakota", "Dakota", "Darwin", "Sunny", "Oscar", "Sunny");
		    JavaRDD<String> petsRDD = sc.parallelize(thePetsList);
			
		    
		    long rddCount = petsRDD.count();
		    logger.info("TOTAL COUNT=" + rddCount);
		    
		    logger.info("First output");
		    Map<String,Long> countByValue = petsRDD.countByValue();
		    for (Map.Entry<String, Long> entry : countByValue.entrySet()) {
		    	logger.info(entry.getKey() + "=" + entry.getValue());
		    }
		    
		    logger.info("Second output");
		    for (String key : countByValue.keySet()) {
		    	Long value = countByValue.get(key);
		    	logger.info(key + "=" + value);
		    }
		    

			logger.info("=== COUNT TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testTake() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== TAKE TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("takeTest").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<String> thePetsList = Arrays.asList("Captain", "Dakota", "Dakota", "Impy", "Tweety", "Darwin", "Sunny", "Oscar", "Sunny");
		    JavaRDD<String> petsRDD = sc.parallelize(thePetsList);
			
		   
		    List<String> takeList = petsRDD.take(3);
		    for (String pet : takeList) {
		    	logger.info("Next take=" + pet);
		    }

			logger.info("=== TAKE TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testReduce() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== REDUCE TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("testReduce").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<Integer> integerList = Arrays.asList(1,2,3,4,5);
		    JavaRDD<Integer> integerRDD = sc.parallelize(integerList);
		    Integer product = integerRDD.reduce( (x,y) -> x * y);
		    logger.info("Product=" + product);
			logger.info("=== REDUCE TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testReduce2() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== REDUCE2 TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("testReduce").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<Integer> integerList = Arrays.asList(1,2,3,4,5);
		    JavaRDD<Integer> integerRDD = sc.parallelize(integerList);
		    Integer product = integerRDD.reduce( (x,y) -> {
		    	logger.info("Called with->" + x + " " + y);
		    	return x * y;
		    });
		    
		    
		    logger.info("Product=" + product);
			logger.info("=== REDUCE2 TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	// @Ignore
	public void testSumOfPrimes1() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== SUM OF PRIMES TEST BEGIN");
		    SparkConf conf = new SparkConf().setAppName("testSumOfPrimes").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    JavaRDD<String> inputLines = sc.textFile("input/prime_nums.text");
		    JavaRDD<Integer> primes = inputLines.flatMap(line -> {
		    	List<String> primeListStrings = Arrays.asList(line.split(WS));
		    	List<Integer> primeListIntegers = new ArrayList<Integer>();
		    	for (String next : primeListStrings) {
		    		if (next != null && !next.trim().equals("")) {
		    			Integer nextPrime = Integer.valueOf(next);
		    			primeListIntegers.add(nextPrime);
		    		}
		    	}
		    	return primeListIntegers.iterator();
		    });
		    
		    
		    Integer sumOfPrimes = primes.reduce( (x,y) -> {
		    	return x+y;
		    });
		    logger.info("Sum Of Primes=" + sumOfPrimes);
		    
		    
			logger.info("=== SUM OF PRIMES TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testSumOfPrimes2() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== SUM OF PRIMES TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("primeNumbers").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);

	        JavaRDD<String> lines = sc.textFile("input/prime_nums.text");
	        JavaRDD<String> numbers = lines.flatMap(line -> Arrays.asList(line.split("\\s+")).iterator());

	        JavaRDD<String> validNumbers = numbers.filter(number -> !number.isEmpty());

	        JavaRDD<Integer> intNumbers = validNumbers.map(number -> Integer.valueOf(number));

	        logger.info("Sum is: " + intNumbers.reduce((x, y) -> x + y));

			logger.info("=== SUM OF PRIMES TEST END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	
}
