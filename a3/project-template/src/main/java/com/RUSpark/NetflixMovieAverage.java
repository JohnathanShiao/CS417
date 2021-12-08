package com.RUSpark;

/* any necessary Java packages here */
import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.List;
import java.math.RoundingMode;
import java.text.DecimalFormat;

public class NetflixMovieAverage {

	public static void main(String[] args) throws Exception {

    if (args.length < 1) {
      System.err.println("Usage: NetflixMovieAverage <file>");
      System.exit(1);
    }
		
		String InputPath = args[0];
		
		/* Implement Here */ 
		SparkSession spark = SparkSession.builder()
      .appName("MovieAverage")
      .getOrCreate();

      JavaRDD<String> lines = spark.read().textFile(InputPath).javaRDD();
      JavaRDD<List<String>> lists = lines.map(line -> Arrays.asList(line.split(",")));
      
      JavaPairRDD<Integer,Double> pairs = lists.mapToPair(s -> new Tuple2<>(Integer.parseInt(s.get(0)),Double.parseDouble(s.get(2))));
      JavaPairRDD<Integer,Double> counts = pairs.keys().mapToPair(s->new Tuple2<>(s,1.0)).reduceByKey((i1,i2)->i1+i2).sortByKey();
      JavaPairRDD<Integer, Double> sum = pairs.reduceByKey((i1,i2)->i1+i2).sortByKey();

      JavaPairRDD<Integer, Double> average = counts.join(sum).mapToPair(s-> new Tuple2<>(s._1(),s._2()._2() / s._2()._1())).sortByKey();
      List<Tuple2<Integer,Double>> output = average.collect();
      DecimalFormat df = new DecimalFormat("#.##");
      df.setRoundingMode(RoundingMode.FLOOR);
      for(Tuple2<?,?> tuple: output)
      {
        if((double)tuple._2() > 4.5)
          System.out.println(tuple._1() + " " + df.format(tuple._2()));
      }
      spark.close();
	}

}
