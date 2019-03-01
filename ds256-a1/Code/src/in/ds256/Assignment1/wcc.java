package in.ds256.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class wcc {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS
        Long vertexCount = Long.parseLong(args[2]); // No. of Vertices

        boolean hasConverged = false;

        SparkConf sparkConf = new SparkConf().setAppName("WCC");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputRDD = sc.textFile(inputFile);

        List<Long> vertices = LongStream.rangeClosed(1, vertexCount).boxed().collect(Collectors.toList());

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> vertexRDD = sc.parallelize(vertices).mapToPair(vertex -> new Tuple2<>(vertex, new Tuple3<>(new ArrayList<>(), true, vertex)));

        // SCHEMA : Tuple2<SourceID, TargetID>
        JavaPairRDD<Long, Long> edgeRDD = inputRDD.flatMapToPair(edge -> {
            String[] tokens = edge.split(" ");
            ArrayList<Tuple2<Long, Long>> e = new ArrayList<>();
            if (tokens.length == 2) {
                e.add(new Tuple2<> (Long.parseLong(tokens[0]), Long.parseLong(tokens[1])));
                e.add(new Tuple2<> (Long.parseLong(tokens[1]), Long.parseLong(tokens[0])));
                return e.iterator();
            }
            return null;
        });

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> adjRDD = edgeRDD.groupByKey().mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple3<>(Lists.newArrayList(vertex._2), true, vertex._1)));

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
        vertexRDD = vertexRDD.union(adjRDD).groupByKey().mapToPair(vertex -> {
            ArrayList<Long> adjList = new ArrayList<>();
            for (Tuple3<ArrayList<Long>, Boolean, Long> val : vertex._2) {
                adjList.addAll(val._1());
            }
            return new Tuple2<> (vertex._1, new Tuple3<> (adjList, false, vertex._1));
        });

        while(!hasConverged) {

           // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
           JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> messageRDD = vertexRDD.flatMapToPair(vertex -> {
               ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>> m = new ArrayList<>();
               for(Long v: vertex._2._1()) {
                   m.add(new Tuple2<>(v, new Tuple3<>(null, true, vertex._2._3())));
               }
               return m.iterator();
           });

           // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isActive, vertexState>>
           vertexRDD = vertexRDD.union(messageRDD).groupByKey().mapToPair(vertex -> {
               Long maximumValue = 0L;
               Long currentValue = 0L;
               boolean hasChanged = false;
               ArrayList<Long> adjList = new ArrayList<>();
               for(Tuple3<ArrayList<Long>, Boolean, Long> val: vertex._2) {
                   if (! (val._1() == null)) {
                       currentValue = val._3();
                       adjList = val._1();
                   }
                   if (val._3() > maximumValue) {
                       maximumValue = val._3();
                   }
               }
               if (maximumValue > currentValue) {
                   hasChanged = true;
                   currentValue = maximumValue;
               }
               return new Tuple2<>(vertex._1, new Tuple3<>(adjList, hasChanged, currentValue));
           });

           hasConverged = vertexRDD.filter(vertex -> vertex._2._2()).take(1).isEmpty();

        }

        vertexRDD.mapToPair(vertex ->  new Tuple2<>(vertex._2._3(), vertex._1)).sortByKey().saveAsTextFile(outputFile);

        sc.stop();
        sc.close();

    }
}
