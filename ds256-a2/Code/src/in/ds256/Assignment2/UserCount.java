package in.ds256.Assignment2;

import java.util.*;
import java.util.regex.Pattern;

import com.google.common.hash.HashFunction;
import scala.Array;
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

import static com.google.common.hash.Hashing.murmur3_32;

public class UserCount {

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

		/**
		*	Code goes here....
		*/

		JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	private static Long getDistinctCount(Iterator<String> x) {

		// Common

        // Hash Functions
        ArrayList<HashFunction> hf = new ArrayList<>();
        for(int i=0; i<30; i++) {
           hf.add(murmur3_32(12345));
        }

        // Signatures
        ArrayList<Integer> sig = new Array

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
