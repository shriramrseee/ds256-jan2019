package in.ds256.Assignment0;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.json.simple.JSONArray;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;

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
        JavaRDD<String> twitterData = sc.textFile(inputFile);

        System.out.println("Shriram: File Opened !");

        // Convert to json
        JavaRDD<JSONObject> parsedData = twitterData.map((Function<String, JSONObject>) x -> (JSONObject) new JSONParser().parse(x));

        System.out.println("Shriram: Parsed JSON !");

        // Get Hash Count
        JavaRDD<Integer> hashCount = parsedData.map((Function<JSONObject, Integer>) x -> ((JSONArray)((JSONObject) x.get("entities")).get("hashtags")).size());

        System.out.println("Shriram: Obtained Count !");

        // Save file
        hashCount.saveAsTextFile(outputFile);

        System.out.println("Shriram: Saved Output !");

        sc.stop();
        sc.close();
    }

}