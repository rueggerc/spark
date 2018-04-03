package com.rueggerllc.spark.sparkSQL;

import static org.apache.spark.sql.functions.col;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.rueggerllc.spark.sparkSQL.delegates.EmployeeJoiner;

/*
 * Spark SQL supports same basic join types as core spark
 * Spark SQL Catalyst optimizer can do more heavy lifting to optimize join performance
 * We have to give up some control:
 * SparkSQL can sometimes push down or re-order operations to make joins more efficient.
 * We don't have controls over partitioners for Data Sets
 * Cant manually avoid shuffles as we can with core Spark Joins
 * 
 * Join Types:
 * standard SQL join types are supported, can be specified as joint type:
 * inner
 * outer 
 * left outer
 * right outer
 * left semi
 *
 * 
 */

public class SparkJoins {


    public static void main(String[] args) throws Exception {

        Logger.getLogger("org").setLevel(Level.ERROR);
        
        System.out.println("================ SPARK SQL BEGIN ===============");
        
        SparkSession session = SparkSession.builder().appName("SparkJoins").master("local[*]").getOrCreate();
        // SparkSession session = SparkSession.builder().appName("SparkJoins").getOrCreate();

        DataFrameReader dataFrameReader = session.read();

        System.out.println("================ READING DATA ===============");
        // Uses Header to infer schema of DataSet instead of providing schema manually.
        // Simplifies things
        Dataset<Row> nameAgeTable = dataFrameReader.option("header","true").csv("input/name_age.csv");
        Dataset<Row> nameCountryTable = dataFrameReader.option("header","true").csv("input/name_country.csv");
        
        // Inner Join
        EmployeeJoiner employeeJoiner = new EmployeeJoiner();
        employeeJoiner.innerJoin(nameAgeTable, nameCountryTable);
        
        session.stop();
    }
}
