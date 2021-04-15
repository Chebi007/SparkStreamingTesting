package com.javastreaming;

import static com.javastreaming.JavaStreamingApp.*;

import com.holdenkarau.spark.testing.JavaStreamingSuiteBase;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;


public class SampleJavaStreamingTest extends JavaStreamingSuiteBase implements Serializable {

    @Test
    public void verifyMapTest() {
        List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2, 6), Arrays.asList(3, 4, 5, 8));
        List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(2, 4, 12), Arrays.asList(6, 8, 10, 16));

        testOperation(input, mapOperation, expectedOutput);
        testOperation(input, mapOperation, expectedOutput, true);
    }

    @Test
    public void verifyFilterTest() {
        List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2, 6), Arrays.asList(3, 4, 5, 8));
        List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(2, 6), Arrays.asList(4, 8));

        testOperation(input, filterOddOperation, expectedOutput);
        testOperation(input, filterOddOperation, expectedOutput, true);
    }

    @Test
    public void verifyBinaryOperation() {
        List<List<Integer>> input1 = Arrays.asList(Arrays.asList(1), Arrays.asList(3));
        List<List<Integer>> input2 = Arrays.asList(Arrays.asList(2), Arrays.asList(4));
        List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(1, 2), Arrays.asList(3, 4));

        testOperation(input1, input2, unionOperation, expectedOutput);
        testOperation(input1, input2, unionOperation, expectedOutput, true);
    }

    @Test
    public void verifyReduceTest() {
        List<List<Integer>> input = Arrays.asList(Arrays.asList(1, 2, 6), Arrays.asList(3, 4, 5, 8));
        List<List<Integer>> expectedOutput = Arrays.asList(Arrays.asList(9), Arrays.asList(20));

        testOperation(input, reduceOperation, expectedOutput);
        testOperation(input, reduceOperation, expectedOutput, true);
    }

}
