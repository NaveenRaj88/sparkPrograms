package com.simpleapp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by Naveen on 07-04-2017.
 */
public class WordCount {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("D:\\spark\\derby.log");

//        JavaRDD words =lines.flatMap(line -> Arrays.asList(line.split(" ")));

        JavaPairRDD<String, Integer> counts = lines.mapToPair(s -> new Tuple2<String, Integer>(s, 1)).reduceByKey((x, y) -> x + y);

//        counts.saveAsTextFile("D:\\counts.txt");

        System.out.println(counts);

    }
}
