package com.simpleapp;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by Naveen on 07-04-2017.
 */
public class SimpleApp {

    public static void main(String[] args) {
        String logFile =  "D:\\spark\\README.md";
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> logData = sc.textFile(logFile).cache();

        long numAs =  logData.filter(line -> line.contains("a")).count();

        long numBs = logData.filter(line -> line.contains("b")).count();

        System.out.println("Lines with a: "+ numAs+" Lines with b: "+ numBs);
        sc.stop();
    }
}
