package in.ds256.Assignment0;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
 * Code for generating interaction graph
 */
public class InterGraph {

    public static void main(String[] args) throws IOException, ParseException {

        String inputFile = args[0]; // Should be some file on HDFS
        String vertexFile = args[1]; // Should be some file on HDFS
        String edgeFile = args[2]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("InterGraph");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> twitterData = sc.textFile(inputFile).cache();
        System.out.println("Shriram: File Opened !");

        // Get vertex info
        JavaRDD<String> vertexInfo = twitterData.flatMapToPair((PairFlatMapFunction<String, Tuple2<Long, Long>, String>) InterGraph::getVertex).reduceByKey((x, y) -> x + "," + y).map(x -> x._1._1 + "," + x._1._2 + "," + x._2);;
        System.out.println("Shriram: Obtained Vertex Info !");

        // Get Edge info


        // Save File
        vertexInfo.saveAsTextFile(vertexFile);


        sc.stop();
        sc.close();
    }

    private static Iterator<Tuple2<Tuple2<Long, Long>, String>> getVertex(String x) throws ParseException {
        try {
            JSONObject j = (JSONObject) new JSONParser().parse(x);
            JSONObject k = (JSONObject) j.get("user");
            DateTimeFormatter dtf  = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss xxxx yyyy");
            Long id = (Long) k.get("id");
            Long created_at = ZonedDateTime.parse((String) k.get("created_at"), dtf).toInstant().toEpochMilli();
            Long ts = ZonedDateTime.parse((String) j.get("created_at"), dtf).toInstant().toEpochMilli();
            Long foc = (Long) k.get("followers_count");
            Long frc = (Long) k.get("friends_count");
            return Collections.singletonList(new Tuple2<>(new Tuple2<>(id, created_at), ts + "," + foc + "," + frc)).iterator();
        } catch (NullPointerException e) {
            return Collections.emptyIterator();
        }
    }

}