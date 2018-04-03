package com.rueggerllc.spark.sparkSQL.delegates;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class EmployeeJoiner {
	
	private static Logger logger = Logger.getLogger(EmployeeJoiner.class);
	private static final String INNER_JOIN = "inner";
	
	/**
	 * Spark Inner Join:
	 * requires that keys are available in both tables
	 * nameAgeTable.col("name").equals((nameCountryTable).col("name")),
	 */
	public void innerJoin(Dataset<Row> nameAgeTable, Dataset<Row> nameCountryTable) {
		try {

			logger.info("INNER JOIN BEGIN");
			
	        // Inferred Schema
	        logger.info("INFERRED SCHEMA");
	        nameAgeTable.printSchema();
	        nameCountryTable.printSchema();
	        
	        // First n records
	        nameAgeTable.show(10);
	        nameCountryTable.show(10);
	        
	        // Inner Join
	        Dataset<Row> joined = 
	        	nameAgeTable.join(nameCountryTable,
	        					  nameAgeTable.col("name").equalTo((nameCountryTable).col("name")),
	        				      INNER_JOIN);
	        joined.show(10);	        
	        
	        			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	/**
	 * Left Outer Join
	 */
	public void leftOuterJoin(Dataset<Row> rows) {
		try {

			logger.info("LEFT OUTER JOIN BEGIN");
			
	        // Inferred Schema
	        logger.info("INFERRED SCHEMA");
	        rows.printSchema();
	        
	        // First n records
	        rows.show(10);
	        
	        			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	/**
	 * Right Outer Join
	 */
	public void rightOuterJoin(Dataset<Row> rows) {
		try {

			logger.info("RIGHT OUTER JOIN BEGIN");
			
	        // Inferred Schema
	        logger.info("INFERRED SCHEMA");
	        rows.printSchema();
	        
	        // First n records
	        rows.show(10);
	        
	        			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}
	
	/**
	 * Left Semi Join
	 */
	public void leftSemiJoin(Dataset<Row> rows) {
		try {

			logger.info("LEFT SEMI JOIN BEGIN");
			
	        // Inferred Schema
	        logger.info("INFERRED SCHEMA");
	        rows.printSchema();
	        
	        // First n records
	        rows.show(10);
	        
	        			
		} catch (Exception e) {
			logger.error("ERROR", e);
		}
	}

}
