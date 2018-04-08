package com.rueggerllc.spark.tests;

import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BasicRDDTests {

	private static Logger logger = Logger.getLogger(BasicRDDTests.class);

	
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
	public void testBasicRDDStuff() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("RDDTests").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        JavaRDD<String> rawLines = sc.textFile("input/rawlines.txt");
	        logger.info("LINE COUNT=" + rawLines.count());
	        
	        JavaRDD<String> rawLinesToUpper = rawLines.map(line -> line.toUpperCase());
	        
	        
	        JavaRDD<String> rawLinesWithLength = rawLines.map(line -> line + ":" + line.length());
	        rawLinesWithLength.foreach(entry -> {
	        	logger.info(entry);
	        });
	        
	       
	        List<String> threeLines = rawLinesToUpper.take(3);
	        for (String line : threeLines) {
	        	logger.info("next 3 line=>" + line);
	        }
	        
	        // Display
	        rawLinesToUpper.foreach(entry -> {
	        	logger.info(entry);
	        });
	        
	        JavaRDD<String> words = rawLinesToUpper.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
	        logger.info("WORD COUNT=" + words.count());
	        
	        
	        // Done
	        sc.close();

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}

		
	
	
}
