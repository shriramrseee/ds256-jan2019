package in.ds256.Assignment2;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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

public class SentimentAnalysis {

	private static String[] sentiments = {"Fear", "Hostility", "Guilt", "Sadness", "Joviality", "Self-Assurance", "Attentiveness", "Shyness", "Fatigue", "Serenity", "Surprise"};

	private static double[] baseline = {0.0063791, 0.0086279, 0.0021756, 0.0018225,	0.0007608, 0.0240757, 0.0084612, 0.0182421, 0.0036012, 0.0008997, 0.0022914};

	private static List<HashSet<String>> terms = Arrays.asList(	new HashSet<>(Arrays.asList("afraid", "scared", "frightened", "nervous", "jittery", "shaky")),
			new HashSet<>(Arrays.asList("angry", "hostile", "irritable", "scornful", "disgusted", "loathing")),
			new HashSet<>(Arrays.asList("guilty", "ashamed", "blameworthy", "angry at self", "disgusted with self", "dissatisfied with self")),
			new HashSet<>(Arrays.asList("sad", "blue", "downhearted", "alone", "lonely")),
			new HashSet<>(Arrays.asList("happy", "joyful", "delighted", "cheerful", "excited", "enthusiastic", "lively", "energetic")),
			new HashSet<>(Arrays.asList("proud", "strong", "confident", "bold", "daring", "fearless")),
			new HashSet<>(Arrays.asList("alert", "attentiveness", "concentrating", "determined")),
			new HashSet<>(Arrays.asList("shy", "bashful", "sheepish", "timid")),
			new HashSet<>(Arrays.asList("sleepy", "tired", "sluggish", "drowsy")),
			new HashSet<>(Arrays.asList("calm", "relaxed", "at ease")),
			new HashSet<>(Arrays.asList("amazed", "surprised", "astonished")));

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
		SparkConf sparkConf = new SparkConf().setAppName("UserCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		
		Map<String, Object> kafkaParams = new HashMap<>();
		kafkaParams.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker);
		kafkaParams.put(ConsumerConfig.GROUP_ID_CONFIG, topic);
		kafkaParams.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		kafkaParams.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

		// Create direct kafka stream with broker and topic
		JavaInputDStream<ConsumerRecord<String, String>> messages = KafkaUtils.createDirectStream(jssc,
				LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(Collections.singleton(topic), kafkaParams));

		/*
		*	Code goes here....
		*/

		JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

        final Long[] count = {0L};

		tweets.foreachRDD( (VoidFunction2<JavaRDD<String>, Time>) (r, time) -> count[0] += r.count());

		JavaPairDStream<Integer, Long> sentiment = tweets.mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, Integer, Long>) SentimentAnalysis::getSentiment);

		sentiment.reduceByKey((x, y) -> x + y).mapToPair(x -> new Tuple2<>(x._1, x._2*1.0/count[0])).mapToPair(x -> {
		    if(x._2 <= baseline[x._1])
		        return new Tuple2<>((baseline[x._1] - x._2) * 1.0 / baseline[x._1], sentiments[x._1]);
		    else
                return new Tuple2<>(-(x._2 - baseline[x._1]) * 1.0 / x._2, sentiments[x._1]);
        }).foreachRDD((VoidFunction2<JavaPairRDD<Double, String>, Time>) (r, time) -> {
            List<Tuple2<Double, String>> result = r.sortByKey(false).take(3);
            Configuration conf = new Configuration();
            String fpath = output+time.toString().split(" ")[0];
            FileSystem fs = FileSystem.get(URI.create(fpath), conf);
            FSDataOutputStream out = fs.create(new Path(fpath));
            for (Tuple2<Double, String> pair : result) {
                out.write((pair._2 + "," + pair._1).getBytes(StandardCharsets.UTF_8));
                out.write(("\n").getBytes(StandardCharsets.UTF_8));
            }
            out.close();
        });

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	private static Iterator<Tuple2<Integer, Long>> getSentiment(Iterator<String> x) {

		// Common
		Long out[] = new Long[sentiments.length];
		for(int i=0; i<sentiments.length; i++) {
			out[i] = 0L;
		}

		String s;
		Set<String> words;
		for (; x.hasNext(); ) {
			s = x.next();
			try {
				words = new HashSet<>(Arrays.asList(s.split(",")[7].split(" ")));
                if (s.split(",").length == 9)
                    words.addAll(new HashSet<>(Arrays.asList(s.split(",")[8].split(";"))));
				for (int i=0; i<terms.size(); i++)
				{
					if (!(Sets.intersection(terms.get(i), words).isEmpty())) {
					    out[i]++;
                    }
				}
			} catch (Exception e) {
				// Do nothing
			}
		}

		ArrayList<Tuple2<Integer, Long>> result = new ArrayList<>();
		for(int i=0; i<out.length; i++) {
			if (out[i] > 0)
		        result.add(new Tuple2<>(i, out[i]));
        }
		return result.iterator();
	}

}
