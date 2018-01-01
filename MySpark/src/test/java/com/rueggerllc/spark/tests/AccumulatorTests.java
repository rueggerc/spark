package com.rueggerllc.spark.tests;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.util.LongAccumulator;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import scala.Option;

public class AccumulatorTests {

	private static Logger logger = Logger.getLogger(AccumulatorTests.class);

	private static String WS = "\\s+";
	public static final String COMMA_DELIMITER = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)";
	

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
	@Ignore
	public void testDummy() {
		logger.info("Dummy Test Begin");
	}
	
	
	

	
	@Test
	// @Ignore
	// Some data is missing from inputs
	// Want to Answer:
	// 1. How many records are in the survey result?
	// 2. How many records are missing the salary middle point?
	// 3. How many records are from Canada?
	public void testStackOverflowSurvey() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("StackOverFlowSurvey").setMaster("local[*]");

	        SparkContext sparkContext = new SparkContext(conf);
	        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkContext);

	        final LongAccumulator total = new LongAccumulator();
	        final LongAccumulator missingSalaryMidPoint = new LongAccumulator();

	        total.register(sparkContext, Option.apply("total"), false);
	        missingSalaryMidPoint.register(sparkContext, Option.apply("missing salary middle point"), false);

	        JavaRDD<String> responseRDD = javaSparkContext.textFile("input/2016-stack-overflow-survey-responses.csv");

	        JavaRDD<String> responseFromCanada = responseRDD.filter(response -> {
	            String[] splits = response.split(COMMA_DELIMITER, -1);

	            logger.info("Inside Filter!");
	            total.add(1);

	            if (splits[14].isEmpty()) {
	                missingSalaryMidPoint.add(1);
	            }

	            return splits[2].equals("Canada");

	        });

	        logger.info("Results:\n");
	        logger.info("Count of responses from Canada: " + responseFromCanada.count());
	        logger.info("Total count of responses: " + total.value());
	        logger.info("Count of responses missing salary middle point: " + missingSalaryMidPoint.value());
	        
	        
	        
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	
	
}
