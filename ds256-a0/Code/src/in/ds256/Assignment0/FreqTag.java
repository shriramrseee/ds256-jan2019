package in.ds256.Assignment0;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;

/**
 * DS-256 Assignment 0
 * Code for generating frequency distribution per hashtag
 */
public class FreqTag {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> textFile = sc.textFile(inputFile);

        // Save file
        textFile.saveAsTextFile(outputFile);

        sc.stop();
        sc.close();
    }

}