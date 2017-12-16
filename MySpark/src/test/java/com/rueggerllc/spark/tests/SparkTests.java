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

public class SparkTests {

	private static Logger logger = Logger.getLogger(SparkTests.class);

	
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
	@Ignore
	public void testMapStringToIntLamda() {
		try {
			String[] values = {"Hello", "Dakota", "Tampa", "Florida"};
			List<String> theWords = Arrays.asList(values);
			Foo foo = new Foo();
			List<Integer> lengths = foo.runIt((s) -> {return s.length();}, theWords);
			for (Integer x : lengths) {
				logger.info("Next Length=" + x);
			}
		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test 
	public void testMapStringToIntAnonmyousInnerClass() {
		try {
			String[] values = {"Hello", "Dakota", "Tampa", "Florida"};
			List<String> theWords = Arrays.asList(values);
			Foo foo = new Foo();
			List<Integer> lengths = foo.runIt(new MyMappingFunction<String,Integer>() {
				@Override
				public Integer mapIt(String input) {
					logger.info("Inside our Just In Time Function!");
					return input.length();
				}
			},theWords);
			
			for (Integer x : lengths) {
				logger.info("Next Length=" + x);
			}
		} catch (Exception e) {
			logger.error("Error", e);
		}
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
	
	@Test
	@Ignore
	public void testGetBigDecimal() {
		try {
			BigDecimal forty = BigDecimal.valueOf(40.0d);
			String stringValue = "-6.569828";
			BigDecimal value = BigDecimal.valueOf((Double.valueOf(stringValue)));
			logger.info("Value=" + value);
			logger.info("Result=" + (value.compare(forty) > 0));
			
		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	public void testGetAirportsLatitudeExceeds40() {
		
        // Each row of the input file contains the following columns:
        // Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
        // ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
		try {
			
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("airportsInUSA").setMaster("local[2]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        JavaRDD<String> airports = sc.textFile("input/airports.text");
	        
	        // Double forty = Double.valueOf(40.0d);
	        JavaRDD<String> airportsInUSA = airports.filter(line -> {
	        	Double latitude = Double.valueOf(line.split(Utils.COMMA_DELIMITER)[6]);
	        	return latitude.compareTo(40.0) > 0;
	        });

	        JavaRDD<String> airportsLatitudeAbove40 = airportsInUSA.map(line -> {
	                    String[] splits = line.split(Utils.COMMA_DELIMITER);
	                    return StringUtils.join(new String[]{splits[1], splits[6]}, ",");
	                }
	        );
	        airportsLatitudeAbove40.saveAsTextFile("output/airports_by_latitude.text");

		} catch (Exception e) {
			logger.error("Error", e);
		}
		
	}
		
	
	
}
