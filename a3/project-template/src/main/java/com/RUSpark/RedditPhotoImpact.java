package com.RUSpark;

/* any necessary Java packages here */

import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class RedditPhotoImpact {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) 
    {
      System.err.println("Usage: RedditPhotoImpact <file>");
      System.exit(1);
    }
  
    String InputPath = args[0];

    SparkSession spark = SparkSession
        .builder()
        .appName("PhotoImpact")
        .getOrCreate();

    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
    JavaRDD<List<String>> lists = lines.map(line -> Arrays.asList(line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)",-1)));
    JavaPairRDD<Integer,Integer> pairs = lists.mapToPair(s-> new Tuple2<>(Integer.parseInt(s.get(0)),Integer.parseInt(s.get(4)) + Integer.parseInt(s.get(5)) + Integer.parseInt(s.get(6))));
    JavaPairRDD<Integer, Integer> counts = pairs.reduceByKey((i1,i2)->i1+i2).sortByKey();
    List<Tuple2<Integer,Integer>> output = counts.collect();
    int max = 0;
    for(Tuple2<?,?> tuple: output)
    {
      System.out.println(tuple._1() + " " + tuple._2());
      if((int)tuple._2()>max)
        max= (int)tuple._1();
    }
    System.out.println("THIS IS THE MAX " + max);
    spark.close();
	}

}
