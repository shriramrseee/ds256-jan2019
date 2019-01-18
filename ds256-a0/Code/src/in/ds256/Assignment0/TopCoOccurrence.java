package in.ds256.Assignment0;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import scala.Tuple2;

/**
 * DS-256 Assignment 0
 * Code for finding frequent co-occurring hash-tags
 */
public class TopCoOccurrence {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("​TopCoOccurrence");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> twitterData = sc.textFile(inputFile);
        System.out.println("Shriram: File Opened !");

        // Get Hash tags
        JavaPairRDD<Tuple2<String, String>, Long> hashTags = twitterData.flatMapToPair((PairFlatMapFunction<String, Tuple2<String, String>, Long>) TopCoOccurrence::getHashTags);
        System.out.println("Shriram: Obtained Hash Tags !");

        // Get count
        JavaPairRDD<Long, Tuple2<String, String>> hashCount = hashTags.reduceByKey((x, y) -> x + y).mapToPair( x -> new Tuple2<>(x._2, x._1) ).sortByKey();
        System.out.println("Shriram: Obtained Count !");

        // Get top 100
        List<Tuple2<Long, Tuple2<String, String>>> pairs = hashCount.take(100);

        // Save file
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(outputFile), conf);
        FSDataOutputStream out = fs.create(new Path(outputFile));
        for (Tuple2<Long, Tuple2<String, String>> pair : pairs) {
            out.write((pair._1 + ",").getBytes());
            out.write((pair._2._1 + ",").getBytes());
            out.write((pair._2._2 + ",").getBytes());
            out.write(("\n").getBytes());
        }
        out.close();

        System.out.println("Shriram: Output written to file !");


        sc.stop();
        sc.close();
    }

    private static Iterator<Tuple2<Tuple2<String, String>, Long>> getHashTags(String x) throws ParseException {
        try {
            JSONObject j = (JSONObject) new JSONParser().parse(x);
            JSONArray a = (JSONArray) ((JSONObject) j.get("entities")).get("hashtags");
            String[] as = new String[a.size()];
            for(int i = 0; i < a.size(); i++)
                as[i] = (String) ((JSONObject) a.get(i)).get("text");
            List<Tuple2<Tuple2<String, String>, Long>> ht = new ArrayList<>();
            for (int i=0; i<as.length; i++) {
                for (int k=0; k<as.length; k++) {
                    if (as[i].compareTo(as[k]) > 0 ) {
                        ht.add(new Tuple2<>(new Tuple2<>(as[i], as[k]), 1L));
                    }
                }
            }
            return ht.iterator();
        } catch (NullPointerException e) {
            return Collections.emptyIterator();
        }
    }

}