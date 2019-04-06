package in.ds256.Assignment2;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.Time;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.Durations;

public class TweetsETL {

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
		SparkConf sparkConf = new SparkConf().setAppName("TweetETL");
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
			Code goes here....
		*/

		JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

		JavaDStream<String> etl = tweets.mapPartitions((FlatMapFunction<Iterator<String>, String>) TweetsETL::performETL);

		etl.dstream().saveAsTextFiles(output,"txt");

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}
	
	private static String[] stop_words = {"a","about","above","after","again","against","all","am","an","and","any","are","aren't","as","at","be","because","been","before","being","below","between","both","but","by","can't","cannot","could","couldn't","did","didn't","do","does","doesn't","doing","don't","down","during","each","few","for","from","further","had","hadn't","has","hasn't","have","haven't","having","he","he'd","he'll","he's","her","here","here's","hers","herself","him","himself","his","how","how's","i","i'd","i'll","i'm","i've","if","in","into","is","isn't","it","it's","its","itself","let's","me","more","most","mustn't","my","myself","no","nor","not","of","off","on","once","only","or","other","ought","our","ours","ourselves","out","over","own","same","shan't","she","she'd","she'll","she's","should","shouldn't","so","some","such","than","that","that's","the","their","theirs","them","themselves","then","there","there's","these","they","they'd","they'll","they're","they've","this","those","through","to","too","under","until","up","very","was","wasn't","we","we'd","we'll","we're","we've","were","weren't","what","what's","when","when's","where","where's","which","while","who","who's","whom","why","why's","with","won't","would","wouldn't","you","you'd","you'll","you're","you've","your","yours","yourself","yourselves"};

	private static Iterator<String> performETL(Iterator<String> x) {
		JSONParser jp = new JSONParser();

		// Common
        ArrayList<String> out = new ArrayList<>();
		DateTimeFormatter dtf = DateTimeFormatter.ofPattern("EEE MMM dd HH:mm:ss xxxx yyyy");
        String s;
		String tweet;
        String parsed;
	    for (; x.hasNext(); ) {
			s = x.next();
			try {
				JSONObject j = (JSONObject) jp.parse(s);
                JSONObject k = (JSONObject) j.get("user");
                parsed = "";
                // Time
                parsed = parsed.concat(ZonedDateTime.parse((String) j.get("created_at"), dtf).toInstant().toEpochMilli() + ",");
                parsed = parsed.concat(ZonedDateTime.parse((String) j.get("created_at"), dtf).getZone().getId() + ",");
                // Lang
                parsed = parsed.concat(j.get("lang") + ",");
                // ID
                parsed = parsed.concat(j.get("id_str") + ",");
                parsed = parsed.concat(k.get("id_str") + ",");
                // Count
                parsed = parsed.concat(k.get("friends_count") + ",");
                parsed = parsed.concat(k.get("followers_count") + ",");
                // Tweet
                tweet = ((String) j.get("text")).toLowerCase();
                for(String st: stop_words) {
                    tweet = tweet.replaceAll(st+" ", "");
                    tweet = tweet.replaceAll(" "+st, "");
                }
                tweet = tweet.replaceAll("[^a-zA-Z ]", "");
                tweet = tweet.replaceAll(" {2}", " ");
                parsed = parsed.concat(tweet + ",");
				// Hash tags
				JSONArray h = (JSONArray) ((JSONObject) j.get("entities")).get("hashtags");
                for (Object aH : h) parsed = parsed.concat(((JSONObject) aH).get("text") + ";");
                out.add(parsed);
			} catch (Exception e) {
				// Do nothing
			}
		}
		return out.iterator();
	}

}
