package in.ds256.Assignment2;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.*;

import com.google.common.hash.HashFunction;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function3;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import scala.Tuple2;

import static com.google.common.hash.Hashing.murmur3_32;

public class UserCount {

    private static int no_of_hashes = 2700;
    private static int no_of_bins = 15;
    private static int bin_size = 180;

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
		jssc.checkpoint(".");
		
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

		// Initialize state
        List<Tuple2<Integer, Integer>> hashes = new ArrayList<>();
        for(int i=0; i<no_of_hashes; i++) {
            hashes.add(new Tuple2<>(i, 0));
        }
        JavaPairRDD<Integer, Integer> hashRDD = jssc.sparkContext().parallelizePairs(hashes);

		// Process new batch
        JavaDStream<String> tweets = messages.map(ConsumerRecord::value);

        JavaPairDStream<Integer, Integer> batchHashes = tweets.mapPartitionsToPair((PairFlatMapFunction<Iterator<String>, Integer, Integer>) UserCount::getHash);

        // Update state
        Function3<Integer, Optional<Integer>, State<Integer>, Tuple2<Integer, Integer>> mappingFunc =
                (index, hash, state) -> {
                    int m = Math.max(hash.orElse(0), (state.exists() ? state.get() : 0));
                    Tuple2<Integer, Integer> result = new Tuple2<>(index, m);
                    state.update(m);
                    return result;
                };

        JavaMapWithStateDStream<Integer, Integer, Integer, Tuple2<Integer, Integer>> streamHashState = batchHashes.mapWithState(StateSpec.function(mappingFunc).initialState(hashRDD));


        // Compute distinct count and persist
        streamHashState.foreachRDD((VoidFunction2<JavaRDD<Tuple2<Integer, Integer>>, Time>) (r, time) -> {
            List<Tuple2<Integer, Integer>> values  = r.collect();
            Configuration conf = new Configuration();
            String fpath = output+time.toString().split(" ")[0];
            FileSystem fs = FileSystem.get(URI.create(fpath), conf);
            FSDataOutputStream out = fs.create(new Path(fpath));
            out.write((computeDistinct(values) + "").getBytes(StandardCharsets.UTF_8));
            out.write(("\n").getBytes(StandardCharsets.UTF_8));
            out.close();
        });

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
	}

	private static Iterator<Tuple2<Integer, Integer>> getHash(Iterator<String> x) {

        // Hashes
        HashFunction hf[] = new HashFunction[no_of_hashes];
        Random random = new Random(12345L);
        for (int i = 0; i < no_of_hashes; i++) {
            hf[i] = murmur3_32(random.nextInt());
        }

        int hashes[] = new int[no_of_hashes];
        for (int i = 0; i < no_of_hashes; i++) {
            hashes[i] = 0;
        }

        String s;
        String uid;
        int sig, n;

        for (; x.hasNext(); ) {
            s = x.next();
            try {
                uid = s.split(",")[4];
                for (int i = 0; i < no_of_hashes; i++) {
                    sig = hf[i].hashInt(uid.hashCode()).asInt();
                    n = rmo(sig);
                    if (n > hashes[i])
                        hashes[i] = n;
                }
            } catch (Exception e) {
                // Do nothing
            }
        }

        List<Tuple2<Integer, Integer>> result = new ArrayList<>();
        for (int i = 0; i < no_of_hashes; i++) {
            result.add(new Tuple2<>(i, hashes[i]));
        }

        return result.iterator();
    }


    private static Long computeDistinct(List<Tuple2<Integer, Integer>> values) {

        long sum;
        Long avg[] = new Long[no_of_bins];
        for(int i=0; i<no_of_bins; i++) {
            sum = 0L;
            for(int j=0; j<bin_size; j++) {
                sum += 1L << values.get(bin_size*i + j)._2;
            }
            avg[i] = Math.round(sum*1.0/bin_size);
        }

        Arrays.sort(avg);
        return avg[no_of_bins/2];
	}

	private static int rmo(int x) {
		int i = 0, t = 1;
		while(i < 32) {
			if((x & t) != 0)
				break;
			else {
				t = t << 1;
				i++;
			}
		}
		return i;
	}



}
