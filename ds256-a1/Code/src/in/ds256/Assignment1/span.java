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
import scala.Tuple4;

public class span {

	public static void main(String[] args) throws IOException {
		
		String inputFile = args[0]; // Should be some file on HDFS
		String outputFile = args[1]; // Should be some file on HDFS
		Long vertexCount = Long.parseLong(args[2]); // No. of Vertices
		Long sourceVertex = Long.parseLong(args[2]); // Source Vertex

		boolean hasConverged = false;

		SparkConf sparkConf = new SparkConf().setAppName("Span");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);

		JavaRDD<String> inputRDD = sc.textFile(inputFile);

		List<Long> vertices = LongStream.rangeClosed(1, vertexCount).boxed().collect(Collectors.toList());

		// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
		JavaPairRDD<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>> vertexRDD = sc.parallelize(vertices).mapToPair(vertex -> new Tuple2<>(vertex, new Tuple4<>(new ArrayList<>(), !(vertex.equals(sourceVertex)), (vertex.equals(sourceVertex))?0:Long.MAX_VALUE, null)));

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

		// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
		JavaPairRDD<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>> adjRDD = edgeRDD.groupByKey().mapToPair(vertex -> new Tuple2<>(vertex._1, new Tuple4<>(Lists.newArrayList(vertex._2), true, null, null)));

		// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
		vertexRDD = vertexRDD.union(adjRDD).groupByKey().mapToPair(vertex -> {
			ArrayList<Long> adjList = new ArrayList<>();
			Long l = null;
			for (Tuple4<ArrayList<Long>, Boolean, Long, Long> val : vertex._2) {
				adjList.addAll(val._1());
			}
			return new Tuple2<>(vertex._1, new Tuple4<>(adjList, !(vertex._1.equals(sourceVertex)), (vertex._1.equals(sourceVertex))?0:Long.MAX_VALUE, l));
		}).cache();

		while(!hasConverged) {

			// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
			JavaPairRDD<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>> messageRDD = vertexRDD.filter(vertex -> !(vertex._2._2())).flatMapToPair(vertex -> {
				ArrayList<Tuple2<Long, Tuple4<ArrayList<Long>, Boolean, Long, Long>>> m = new ArrayList<>();
					for (Long v : vertex._2._1()) {
						m.add(new Tuple2<>(v, new Tuple4<>(null, false, vertex._2._3(), vertex._1)));
					}
				return m.iterator();
			});

			// SCHEMA : Tuple2<VertexID, Tuple4<List<NeighbourIDs>, isHalted, distance, parent>>
			vertexRDD = vertexRDD.union(messageRDD).groupByKey().repartition(32).mapToPair(vertex -> {
				Long minimumValue = Long.MAX_VALUE;
				Long parentVertex = null;
				Long currentValue = 0L;
				Long currentParentVertex = null;
				boolean hasHalted = true;
				ArrayList<Long> adjList = new ArrayList<>();
				for(Tuple4<ArrayList<Long>, Boolean, Long, Long> val: vertex._2) {
					if (! (val._1() == null)) {
						currentValue = val._3();
						adjList = val._1();
					}
					if (val._3() + 1 < minimumValue) {
						minimumValue = val._3() + 1;
						parentVertex = val._4();
					}
				}
				if (minimumValue < currentValue) {
					hasHalted = false;
					currentValue = minimumValue;
					currentParentVertex = parentVertex;
				}
				return new Tuple2<>(vertex._1, new Tuple4<>(adjList, hasHalted, currentValue, currentParentVertex));
			}).cache();

			hasConverged = vertexRDD.filter(vertex -> !(vertex._2._2())).take(1).isEmpty();

		}

		vertexRDD.map(vertex -> vertex._1.toString() + "," + vertex._2._3().toString() + "," + vertex._2._4().toString()).saveAsTextFile(outputFile);
		
		sc.stop();
		sc.close();
		
	}
}
