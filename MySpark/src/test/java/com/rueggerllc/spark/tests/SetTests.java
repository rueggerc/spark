package com.rueggerllc.spark.tests;

import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;

import org.apache.commons.lang.StringUtils;
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
import com.rueggerllc.spark.functions.MyMappingFunction;
import com.rueggerllc.spark.utils.Utils;

import scala.math.BigDecimal;

public class SetTests {

	private static Logger logger = Logger.getLogger(SetTests.class);

	
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
	public void testGetAirportsInAmerica() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("airportsInUSA").setMaster("local[2]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        JavaRDD<String> airports = sc.textFile("input/airports.text");
	        
	        JavaRDD<String> airportsInUSA = airports.filter(line -> line.split(Utils.COMMA_DELIMITER)[3].equals("\"United States\""));

	        JavaRDD<String> airportsNameAndCityNames = airportsInUSA.map(line -> {
	                    String[] splits = line.split(Utils.COMMA_DELIMITER);
	                    return StringUtils.join(new String[]{splits[1], splits[2]}, ",");
	                }
	        );
	        airportsNameAndCityNames.saveAsTextFile("output/airports_in_usa.text");

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
}
