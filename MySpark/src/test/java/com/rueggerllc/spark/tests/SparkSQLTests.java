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
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.rueggerllc.spark.beans.Foo;
import com.rueggerllc.spark.functions.MyMappingFunction;
import com.rueggerllc.spark.sparkSQL.delegates.EmployeeJoiner;
import com.rueggerllc.spark.utils.Utils;

import scala.math.BigDecimal;

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
	public void testBasic() {
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

	
}
