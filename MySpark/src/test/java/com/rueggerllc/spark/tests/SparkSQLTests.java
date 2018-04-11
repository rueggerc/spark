package com.rueggerllc.spark.tests;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.sum;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.spark.sparkSQL.delegates.EmployeeJoiner;

public class SparkSQLTests {

	private static Logger logger = Logger.getLogger(SparkSQLTests.class);

	
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
	public void testBasicCSV() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			System.out.println("================ SPARK SQL BEGIN ===============");
			SparkSession session = SparkSession.builder().appName("SparkJoins").master("local[*]").getOrCreate();
			DataFrameReader dataFrameReader = session.read();

			System.out.println("================ READING DATA ===============");
			Dataset<Row> nameAgeTable = dataFrameReader.option("header","true").csv("input/name_age.csv");
			Dataset<Row> nameCountryTable = dataFrameReader.option("header","true").csv("input/name_country.csv");
			nameAgeTable.coalesce(1).write()
				.format("com.databricks.spark.csv")
				.option("header", "true")
				.save("output/nameage");   
		        
			// Inner Join
			EmployeeJoiner employeeJoiner = new EmployeeJoiner();
			Dataset<Row> result = employeeJoiner.innerJoin(nameAgeTable, nameCountryTable);
			result.show(10);
		        
			session.stop();
		        
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}

	
	@Test
	// @Ignore
	public void testReadJSON() {
		try {
			
			Logger.getLogger("org").setLevel(Level.ERROR);
			System.out.println("================ SPARK SQL BEGIN ===============");
			SparkSession session = SparkSession.builder().appName("SparkSQLTests").master("local[*]").getOrCreate();
			DataFrameReader dataFrameReader = session.read();

			// Create Data Frame
			Dataset<Row> pets = dataFrameReader.schema(buildSchema()).json("input/pets.json");
			
			// Schema
			pets.printSchema();
			pets.show(10);
		    
	        // SELECT * 
	        // FROM pets
	        // WHERE species='canine'
	        System.out.println("=== Display Canines ===");
	        pets.filter(col("species").equalTo("canine")).show();

			// SELECT avg(weight)
	        // FROM pets
	        pets.agg(sum("weight")).show();
	        
	        pets.agg(avg("weight")).show();
	        
	        
			session.stop();
		        
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	private static StructType buildSchema() {
	    StructType schema = new StructType(
	        new StructField[] {
	        	DataTypes.createStructField("id", DataTypes.IntegerType, false),
	            DataTypes.createStructField("species", DataTypes.StringType, false),
	            DataTypes.createStructField("color", DataTypes.StringType, false),
	            DataTypes.createStructField("weight", DataTypes.DoubleType, false),
	            DataTypes.createStructField("name", DataTypes.StringType, false)});
	    return (schema);
	}

	
}
