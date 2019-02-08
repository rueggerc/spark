package com.rueggerllc.spark.batch;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public class AveragePlayerScorePerTeam {
	
	private static final Logger logger = Logger.getLogger(AveragePlayerScorePerTeam.class);

    public static void main(String[] args) throws Exception {
        Logger.getLogger("org").setLevel(Level.ERROR);
        
        logger.info("==== AveragePlayerScorePerTeam BEGIN ====");
        
        // Get Spark Context
        SparkConf conf = new SparkConf().setAppName("AveragePlayerScoresPerTeam").setMaster("local[*]");
	    JavaSparkContext sc = new JavaSparkContext(conf);	   

	    // Get Data
	    // Method1. Create Pair RDDs from List<Tuple2>
	    // Method2: Transform RDD into PairRDD
	    List<Tuple2<String,Double>> playerScoresList = new ArrayList<>();
	    playerScoresList.add(new Tuple2<String,Double>("TeamA", 5.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamB", 3.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamB", 7.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamA", 9.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamC", 6.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamC", 12.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamC", 6.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamB", 4.0));
	    playerScoresList.add(new Tuple2<String,Double>("TeamA", 1.0));
	    JavaPairRDD<String,Double> playerScores = sc.parallelizePairs(playerScoresList);
	    
	    JavaRDD<Tuple2<String, Double>> averages = playerScores
		    	.mapToPair(new MyMapFunction())
		    	.reduceByKey(new TeamTotalReducer())
		    	.map(new AnswerMapper());
	    
	    // Write To Sink(s)
	    for (Tuple2<String,Double> next : averages.collect()) {
	    	System.out.println("TEAM=" + next._1() + " Average Score=" + next._2());
	    }
    }
    
    private static class AnswerMapper implements Function<Tuple2<String,Tuple2<Integer,Double>>,Tuple2<String,Double>> {
		@Override
		public Tuple2<String, Double> call(Tuple2<String, Tuple2<Integer, Double>> teamData) throws Exception {
			String team = teamData._1();
			Integer count = teamData._2()._1();
			Double total = teamData._2()._2();
			Double average = total/count;
			Tuple2<String,Double> teamAverageScore = new Tuple2<>(team,average);
			return teamAverageScore;
		}
    	
    }
    
    private static class MyMapFunction implements PairFunction<Tuple2<String,Double>,String,Tuple2<Integer,Double>> {
		@Override
		public Tuple2<String, Tuple2<Integer, Double>> call(Tuple2<String, Double> input) throws Exception {
			String teamName = input._1();
			Tuple2<Integer,Double> runningTotals = new Tuple2<Integer,Double>(new Integer(1), input._2());
			return new Tuple2<String,Tuple2<Integer,Double>>(teamName,runningTotals);
		}
    }
    
    private static class TeamTotalReducer implements Function2<Tuple2<Integer,Double>,Tuple2<Integer,Double>,Tuple2<Integer,Double>> {
		@Override
		public Tuple2<Integer, Double> call(Tuple2<Integer, Double> tuple1, Tuple2<Integer, Double> tuple2) throws Exception {
			Integer teamScoreCount = tuple1._1() + tuple2._1();
			Double  teamScoreTotal = tuple1._2() + tuple2._2();
			Tuple2<Integer,Double> runningTotalForTeam = new Tuple2<Integer,Double>(teamScoreCount,teamScoreTotal);
			return runningTotalForTeam;
		}
	}
    

      
    
}