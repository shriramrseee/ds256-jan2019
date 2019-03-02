package in.ds256.Assignment1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import com.google.common.collect.Lists;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

public class conductance {

    public static void main(String[] args) throws IOException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS
        Long vertexCount = Long.parseLong(args[2]); // No. of Vertices

        Random rand = new Random();

        SparkConf sparkConf = new SparkConf().setAppName("Conductance");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputRDD = sc.textFile(inputFile);

        List<Long> vertices = LongStream.rangeClosed(1, vertexCount).boxed().collect(Collectors.toList());

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isSet, degree>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> vertexRDD = sc.parallelize(vertices).mapToPair(vertex -> new Tuple2<>(vertex, new Tuple3<>(new ArrayList<>(), (rand.nextInt(1000) % 3) == 0, 0L)));

        // SCHEMA : Tuple2<SourceID, TargetID>
        JavaPairRDD<Long, Long> edgeRDD = inputRDD.flatMapToPair(edge -> {
            String[] tokens = edge.split("\t");
            ArrayList<Tuple2<Long, Long>> e = new ArrayList<>();
            if (tokens.length == 2) {
                try {
                    e.add(new Tuple2<>(Long.parseLong(tokens[0]), Long.parseLong(tokens[1])));
                    e.add(new Tuple2<>(Long.parseLong(tokens[1]), Long.parseLong(tokens[0])));
                } catch (NumberFormatException n) {
                    return e.iterator();
                }
            }
            return e.iterator();
        });

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isSet, degree>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> adjRDD = edgeRDD.groupByKey().mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple3<>(Lists.newArrayList(vertex._2), true, 1L)));

        // SCHEMA : Tuple2<VertexID, Tuple3<List<NeighbourIDs>, isSet, degree>>
        vertexRDD = vertexRDD.union(adjRDD).groupByKey().mapToPair(vertex -> {
            ArrayList<Long> adjList = new ArrayList<>();
            boolean isSet = true;
            for (Tuple3<ArrayList<Long>, Boolean, Long> val : vertex._2) {
                adjList.addAll(val._1());
                if (val._3() == 0L)
                    isSet = val._2();
            }
            return new Tuple2<>(vertex._1, new Tuple3<>(adjList, isSet, (long) adjList.size()));
        }).cache();

        // Get IN and OUT Degree
        List<Tuple2<Boolean, Long>> inOutDegree;
        inOutDegree = vertexRDD.mapToPair(vertex -> new Tuple2<>(vertex._2._2(), vertex._2._3())).reduceByKey((_1, _2) -> _1 + _2).sortByKey().collect();

        // Get Cross Edges
        // SCHEMA : Tuple2<TargetID, null, isSet, SourceID>>
        JavaPairRDD<Long, Tuple3<ArrayList<Long>, Boolean, Long>> messageRDD = vertexRDD.flatMapToPair(vertex -> {
            ArrayList<Tuple2<Long, Tuple3<ArrayList<Long>, Boolean, Long>>> m = new ArrayList<>();
            for(Long val: vertex._2._1()) {
                if (val < vertex._1)
                    m.add(new Tuple2<>(val, new Tuple3<>(null, vertex._2._2(), vertex._1)));
            }
            return m.iterator();
        });

        Long crossCount = vertexRDD.union(messageRDD).groupByKey().map(vertex -> {
            Long countTrue = 0L;
            Long countFalse = 0L;
            Boolean myValue = null;
            for(Tuple3<ArrayList<Long>, Boolean, Long> val: vertex._2) {
                if(val._1() == null) {
                    if (val._2()) {
                        countTrue++;
                    }
                    else
                        countFalse++;
                }
                else {
                    myValue = val._2();
                }
            }
            return myValue?countFalse:countTrue;
        }).reduce((_1, _2) -> _1 + _2);

        // Compute Conductance
        Long m = (inOutDegree.get(1)._2 < inOutDegree.get(0)._2) ? inOutDegree.get(1)._2 : inOutDegree.get(0)._2;
        Double conductance;
        if (m==0L) {
            conductance =  (crossCount == 0L ? 0.0 : Double.POSITIVE_INFINITY);
        }
        else
            conductance = crossCount * 1.0 / m;

        // Write Output
        vertexRDD.sortByKey().map(vertex -> vertex._1.toString() + ',' + vertex._2._2().toString() + ',' + conductance.toString()).saveAsTextFile(outputFile);

        sc.stop();
        sc.close();

    }
}
