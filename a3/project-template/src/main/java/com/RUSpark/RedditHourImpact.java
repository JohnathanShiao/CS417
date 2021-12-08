package com.RUSpark;


/* any necessary Java packages here */
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

import java.time.Instant;
import java.time.ZoneId;
import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class RedditHourImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: RedditHourImpact <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession.builder()
      .appName("HourImpact")
      .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
    JavaRDD<List<String>> lists = lines.map(line -> Arrays.asList(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1)));
    JavaPairRDD<Integer,Integer> pairs = lists.mapToPair(s-> new Tuple2<>(Instant.ofEpochSecond(Long.parseLong(s.get(1))).atZone(ZoneId.of("America/New_York")).getHour(),Integer.parseInt(s.get(4)) + Integer.parseInt(s.get(5)) + Integer.parseInt(s.get(6))));
    JavaPairRDD<Integer, Integer> counts = pairs.reduceByKey((i1,i2)->i1+i2).sortByKey();
    List<Tuple2<Integer,Integer>> output = counts.collect();
    for(Tuple2<?,?> tuple: output)
    {
      System.out.println(tuple._1() + " " + tuple._2());
    }
    spark.close();
  }
}
