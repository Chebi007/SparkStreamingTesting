package com.javastreaming;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.api.java.JavaDStream;


public class JavaStreamingApp {

    public static void main (String[] args) {
        System.out.println("This is Java Spark Streaming application");
    }

    // .map
    public static Function<JavaDStream<Integer>, JavaDStream<Integer>> mapOperation =
            myInput -> myInput.map((Function<Integer, Integer>) x -> x * 2);

    // .filter
    public static Function<JavaDStream<Integer>, JavaDStream<Integer>> filterOddOperation =
            myInput -> myInput.filter((Function<Integer, Boolean>) x -> x % 2 == 0);

    // .union
    public static Function2<JavaDStream<Integer>, JavaDStream<Integer>, JavaDStream<Integer>> unionOperation =
            JavaDStream::union;

    // .reduce
    public static Function<JavaDStream<Integer>, JavaDStream<Integer>> reduceOperation =
            input1 -> input1.reduce((Function2<Integer, Integer, Integer>) Integer::sum);

}
