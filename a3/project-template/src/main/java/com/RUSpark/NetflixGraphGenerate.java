package com.RUSpark;



/* any necessary Java packages here */
import java.util.*;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;

public class NetflixGraphGenerate {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixGraphGenerate <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		SparkSession spark = SparkSession
      .builder()
      .appName("NetflixGraphGenerate")
      .getOrCreate();
    /* Implement Here */ 
    JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
    JavaRDD<List<String>> lists = lines.map(line->Arrays.asList(line.split(",")));

    JavaPairRDD<Integer,Integer> pairs = lists.mapToPair(s->new Tuple2<>(Integer.parseInt(s.get(0)),Integer.parseInt(s.get(2)))).distinct();
    JavaPairRDD<Tuple2<Integer,Integer>,Integer> ratings = lists.mapToPair(s-> new Tuple2<>(new Tuple2<>(Integer.parseInt(s.get(0)),Integer.parseInt(s.get(2))),Integer.parseInt(s.get(1))));
		JavaPairRDD<Tuple2<Integer,Integer>,Integer> edges = null;

    for(Tuple2<Integer,Integer> tuple: pairs.collect())
    {
      JavaRDD<Integer> customer = ratings.filter(s->(s._1.equals(tuple))).values();
      JavaPairRDD<Tuple2<Integer, Integer>, Integer> newEdges = customer.cartesian(customer).filter(s -> (s._1 < s._2)).mapToPair(m -> new Tuple2(m, 1));
      if(edges == null)
        edges = newEdges;
      else
      {
        edges = edges.union(newEdges);
      }
    }

    JavaPairRDD<Tuple2<Integer,Integer>,Integer> result = edges.reduceByKey((i1,i2) -> i1+i2);
    List<Tuple2<Tuple2<Integer,Integer>,Integer>> output = result.collect();

    
    for(Tuple2<Tuple2<Integer,Integer>,Integer> tuple: output)
    {
      System.out.println("(" + tuple._1()._1() + "," + tuple._1()._2() + ") " + tuple._2());
    }
		spark.stop();
	}

}
