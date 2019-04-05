package in.ds256.Assignment2;


import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

public class HashTagTrends {

	public static void main(String[] args) throws Exception {

		if (args.length < 2) {
			System.err.println("Usage: KafkaWordCount <broker> <topic>\n"
					+ "  <broker> is the Kafka brokers\n"
					+ "  <topic> is the kafka topic to consume from\n\n");
			System.exit(1);
		}
	
		String broker = args[0];
		String topic = args[1];
		String output = args[2];

		// Create context with a 10 seconds batch interval
		SparkConf sparkConf = new SparkConf().setAppName("HashTagTrends");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/**
		*	Code goes here....
		*/

		JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

		JavaPairDStream<String, Long> trending = tweets.mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, String, Long>) HashTagTrends::getHashTags);

		trending = trending.reduceByKeyAndWindow((i1, i2) -> i1 + i2, Durations.seconds(30), Durations.seconds(10));

		trending.mapToPair(x -> new Tuple2<>(x._2, x._1))
				.transformToPair((Function<JavaPairRDD<Long, String>, JavaPairRDD<Long, String>>) x -> x.sortByKey(false))
				.foreachRDD((VoidFunction2<JavaPairRDD<Long, String>, Time>) (r, time) -> {
					List<Tuple2<Long, String>> top  = r.take(5);
					Configuration conf = new Configuration();
					String fpath = output+time.toString().split(" ")[0];
					FileSystem fs = FileSystem.get(URI.create(fpath), conf);
					FSDataOutputStream out = fs.create(new Path(fpath));
					for (Tuple2<Long, String> pair : top) {
						out.write((pair._1 + "," + pair._2).getBytes(StandardCharsets.UTF_8));
						out.write(("\n").getBytes(StandardCharsets.UTF_8));
					}
					out.close();
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	private static Iterator<Tuple2<String, Long>> getHashTags(Iterator<String> x) {

		// Common
		ArrayList<Tuple2<String, Long>> out = new ArrayList<>();
		String s;
		for (; x.hasNext(); ) {
			s = x.next();
			try {
				// Hash tags;
				for (String aH : s.split(",")[8].split(";"))
				{
					if (!aH.equals(""))
					    out.add(new Tuple2<>(aH, 1L));
				}
			} catch (Exception e) {
				// Do nothing
			}
		}
		return out.iterator();
	}
}
