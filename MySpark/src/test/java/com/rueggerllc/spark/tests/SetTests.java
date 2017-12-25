package com.rueggerllc.spark.tests;

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

public class SetTests {

	private static Logger logger = Logger.getLogger(SetTests.class);

	private static String WS = "\\s+";
	
	
	// Set Operations
	// distinct  - expensive, requires shuffling
	// sampling
	// union
	// intersection
	// subtract
	// cartesian product
	
	
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
	public void testGetUnionOfLogFiles() {
		try {
			
			logger.info("=== UNION OF LOG FILES BEGIN");
			
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("airportsInUSA").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        JavaRDD<String> julyFirstLogs = sc.textFile("input/nasa_19950701.tsv");
	        JavaRDD<String> augustFirstLogs = sc.textFile("input/nasa_19950801.tsv");

	        JavaRDD<String> aggregatedLogLines = julyFirstLogs.union(augustFirstLogs);

	        JavaRDD<String> cleanLogLines = aggregatedLogLines.filter(line -> isNotHeader(line));

	        JavaRDD<String> sample = cleanLogLines.sample(true, 0.1);

	        sample.saveAsTextFile("output/sample_nasa_logs.csv");
	        
	        logger.info("=== UNION OF LOG FILES END");
	        

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	// @Ignore
	public void testGetSameHosts() {
		try {
			logger.info("=== GET SAME HOSTS BEGIN");
			
	        // Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("airportsInUSA").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        JavaRDD<String> julyFirstLogs = sc.textFile("input/nasa_19950701.tsv");
	        JavaRDD<String> augustFirstLogs = sc.textFile("input/nasa_19950801.tsv");

	        // Get Hosts from July Log file
	        JavaRDD<String> julyHosts = julyFirstLogs.map(line -> {
                String[] splits = line.split(WS);
                return splits[0];
            });
	        
	        // Output July Hosts
//		    julyHosts.foreach(host -> {
//		        logger.info("Host=" + host);
//		    }); 
		   
	        
	        // Get Hosts from August Log File
	        JavaRDD<String> augustHosts = augustFirstLogs.map(line -> {
                String[] splits = line.split(WS);
                return splits[0];
            });
	        
	        // Get Intersection
	        JavaRDD<String> commonHosts = julyHosts.intersection(augustHosts);
	        
	        
	        // Filter out Headers
	        commonHosts = commonHosts.filter(line -> isNotHostString(line));
	        // JavaRDD<String> cleanedHostIntersection = intersection.filter(host -> !host.equals("host"));

	        commonHosts.saveAsTextFile("output/nasa_same_hosts.csv");
	        
	        logger.info("=== GET SAME HOSTS END");
	        

		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
    private static boolean isNotHeader(String line) {
        return !(line.startsWith("host") && line.contains("bytes"));
    }
	
    
    private static boolean isNotHostString(String line) {
        return !(line.equals("host"));
    }
	
}
