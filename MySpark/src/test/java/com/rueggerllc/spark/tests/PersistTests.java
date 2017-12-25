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
import org.apache.spark.storage.StorageLevel;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

public class PersistTests {

	private static Logger logger = Logger.getLogger(PersistTests.class);

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
	// @Ignore
	public void testPersist1() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== TEST PERSIST1 BEGIN");
		    SparkConf conf = new SparkConf().setAppName("testPersist1").setMaster("local[*]");
		    JavaSparkContext sc = new JavaSparkContext(conf);
		    
		    List<Integer> integerList = new ArrayList<Integer>();
		    for (int i = 1; i <= 10; i++) {
		    	integerList.add(i);
		    }
		    JavaRDD<Integer> integerRDD = sc.parallelize(integerList);
		    
		    // Persist RDD
		    // integerRDD.cache();
		    integerRDD.persist(StorageLevel.MEMORY_ONLY());
		    // integerRDD.persist(StorageLevel.MEMORY_AND_DISK());
		    // integerRDD.persist(StorageLevel.MEMORY_ONLY_SER());
		    // integerRDD.persist(StorageLevel.MEMORY_AND_DISK_SER());
		    // integerRDD.persist(StorageLevel.DISK_ONLY());
		    
		    
		    // Now do multiple actions
		    Integer product = integerRDD.reduce( (x,y) -> x + y);
		    long count = integerRDD.count();
		    
		    logger.info("Product=" + product);
		    logger.info("Count=" + count);
		    
		    logger.info("=== TEST PERSIST1 END");
			

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}


	
	@Test
	@Ignore
	public void testPersistToMemory() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			logger.info("=== SUM OF PRIMES TEST BEGIN");
			
	        SparkConf conf = new SparkConf().setAppName("persistToMemory").setMaster("local[*]");
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
