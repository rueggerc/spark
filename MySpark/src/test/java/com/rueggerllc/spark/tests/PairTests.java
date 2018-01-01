package com.rueggerllc.spark.tests;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.spark.beans.AveragePriceBean;

import scala.Tuple2;

public class PairTests {

	private static Logger logger = Logger.getLogger(PairTests.class);

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
	public void testSortByKey() {
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("testSortByKey").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        List<Tuple2<Integer,String>> priorityNameList = new ArrayList<Tuple2<Integer,String>>();
	        priorityNameList.add(new Tuple2<>(42, "Chris"));
	        priorityNameList.add(new Tuple2<>(12, "Keys"));
	        priorityNameList.add(new Tuple2<>(11, "Grant"));
	        priorityNameList.add(new Tuple2<>(3, "Captain"));
	        JavaPairRDD<Integer, String> priorityNameRDD = sc.parallelizePairs(priorityNameList);

	        JavaPairRDD<Integer, String> sortedPriorityNameRDD = priorityNameRDD.sortByKey();
	        logger.info("Sorted Key/Values BEGIN");
	        for (Tuple2<Integer,String> names : sortedPriorityNameRDD.collect()) {
	        	logger.info(names._1() + ":" + names._2());
	        }
	        logger.info("Sorted Key/Values END");
	        
	        logger.info("Map BEGIN");
	        Map<Integer, String> sortedMap = sortedPriorityNameRDD.collectAsMap();
	        for (Map.Entry<Integer, String> nextEntry : sortedMap.entrySet()) {
	        	logger.info(nextEntry.getKey() + " : " + nextEntry.getValue());
	        }
	       
	        logger.info("Map END");
	        
			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	@Test
	// @Ignore
	public void testWordCountDescendingOrder() {
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("testWordCountDescendingOrder").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        JavaRDD<String> lines = sc.textFile("input/word_count.text");
	        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
		    JavaPairRDD<String, Integer> wordToOneRDD = words.mapToPair(s -> new Tuple2<>(s, 1));
		    JavaPairRDD<String, Integer> wordCountPairRDD = wordToOneRDD.reduceByKey((count1, count2) -> count1 + count2);
		    
		    // Flip to Integer:String and sort
		    JavaPairRDD<Integer, String> countToWordPairs = wordCountPairRDD.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()));
		    JavaPairRDD<Integer, String> sortedCountToWordPairs = countToWordPairs.sortByKey(false);
	        
	        // Flip Back to String:Integer
	        JavaPairRDD<String, Integer> sortedWordToCountRDD = sortedCountToWordPairs.mapToPair(tuple -> new Tuple2<>(tuple._2(), tuple._1()));
		    
	        // Output
	        logger.info("Word ==> Count Descending BEGIN");
	        for (Tuple2<String,Integer> entry : sortedWordToCountRDD.collect()) {
	        	logger.info(entry._1() + ":" + entry._2());
	        }
			
		} catch (Exception e) {
			logger.error("ERROR", e);
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
	@Ignore
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
	
//	@Test
//	// @Ignore
//	public void testReduceByKey() {
//		try {
//			
//			Logger.getLogger("org").setLevel(Level.ERROR);
//			logger.info("=== CONVERT TO PAIR RDD TEST BEGIN");
//			
//	        SparkConf conf = new SparkConf().setAppName("testPairRDD1").setMaster("local[*]");
//	        JavaSparkContext sc = new JavaSparkContext(conf);
//	        
//	        List<String> inputStrings = Arrays.asList("Red 2", "Blue 5", "Blue 1", "Yellow 3", "Red 7", "Red 1", "Blue 2");
//	        JavaRDD<String> regularRDD = sc.parallelize(inputStrings);
//	       
//	        JavaPairRDD<String,Integer> pairRDD = regularRDD.mapToPair(s->new Tuple2<>(getKey(s), getIntegerValue(s)));
//	        
//	        // Reduce By Key
//	        
//	        
//	        
//	        logger.info("=== CONVERT TO PAIR RDD TEST END");
//			
//
//		} catch (Exception e) {
//			logger.error("Error", e);
//		}
//	}
	
	@Test
	@Ignore
	public void testReduceByKey() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);

	        JavaRDD<String> lines = sc.textFile("input/pets.txt");
	        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

	        JavaPairRDD<String, Integer> wordPairRdd = 
	        		words.mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
	        JavaPairRDD<String, Integer> wordCounts = 
	        		wordPairRdd.reduceByKey((Function2<Integer, Integer, Integer>) (x, y) -> x + y);

	        Map<String, Integer> worldCountsMap = wordCounts.collectAsMap();
	        for (Map.Entry<String, Integer> wordCountPair : worldCountsMap.entrySet()) {
	            logger.info(wordCountPair.getKey() + " : " + wordCountPair.getValue());
	        }
	        
	        if (sc != null) {
	        	sc.close();
	        }

	        
		    
		} catch (Exception e) {
			logger.error("Error", e);
		}
	}
	
	@Test
	@Ignore
	//
	// Data Format
	// 0:UniqueID
	// 1:Location
	// 2:Price
	// 3:Number of Bedrooms
	// 4:Number of Bathrooms
	// 5:Square Feet
	// 6:Price Per Square Foot
	// 7:State of Sale
	//
	// Sample:
	// 132842,Arroyo Grande,795000.00,3,3,2371,335.30,Short Sale
	//
	// Problem:
	// Output the average price for houses with different number of Bedrooms
	// Average Price for 3 Bedroom House
	// Average Price for 2 Bedroom House
	// Average Price for 1 Bedroom House
	public void testAverageHousePrice() {
		try {
	        Logger.getLogger("org").setLevel(Level.ERROR);
	        SparkConf conf = new SparkConf().setAppName("averageHousePrice").setMaster("local[*]");
	        JavaSparkContext sc = new JavaSparkContext(conf);
	        
	        // Get input Data
	        JavaRDD<String> lines = sc.textFile("hdfs://captain:9000/inputs/RealEstate.csv");

	        // Filter out Header
	        // JavaRDD<String> lines = sc.textFile("input/RealEstate.csv");
	        lines = lines.filter(line -> filterRealEstateLines(line));
	        
	        // Create Mapping:
	        // # Bedrooms -> (1, price)
	        JavaPairRDD<Integer, AveragePriceBean> averagePriceBeans = lines.mapToPair(
	        		line -> new Tuple2<>(getNumberOfBedrooms(line), new AveragePriceBean(1, getPrice(line)))
	        );
	        
	        // Reduce By Number of Bedrooms
	        // Sum up Average Price Beans for each key
	        JavaPairRDD<Integer, AveragePriceBean> averagePriceTotals = 
	        		averagePriceBeans.reduceByKey( (x,y) -> new AveragePriceBean(x.getCount() + y.getCount(), x.getTotal() + y.getTotal()));
	     
	        // Diagnostics
//	        averagePriceTotals.foreach(entry -> {
//	        	logger.info("Bedrooms=" + entry._1() + " AveragePriceBean=" + entry._2);
//	        });
	        
	        // Compute Average Price for each entry
	        JavaPairRDD<Integer,String> averagePrices = 
	        		averagePriceTotals.mapValues(averagePriceBean -> String.valueOf(averagePriceBean.computeAverage()));
	               
	        averagePrices.foreach(entry -> {
	        	logger.info("Bedrooms=" + entry._1() + " AveragePrice=" + entry._2);
	        });	
	        
	        averagePrices.saveAsTextFile("hdfs://captain:9000/outputs/RealEstate");
	        
	        if (sc != null) {
	        	sc.close();
	        }

	        
		    
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
	
	private static Integer getNumberOfBedrooms(String entry) {
		String[] splits = entry.split(COMMA_DELIMITER);
		Integer bedrooms = Integer.valueOf(splits[3]);
		return bedrooms;
	}
	private static Double getPrice(String entry) {
		String[] splits = entry.split(COMMA_DELIMITER);
		return Double.valueOf(splits[2]);
	}
	
	private static boolean filterRealEstateLines(String line) {
		if (line.trim().equals("")) {
			return false;
		}
		return !(line.split(COMMA_DELIMITER)[0].equals("MLS"));
	}
	
	
}
