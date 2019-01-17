package in.ds256.Assignment0;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.json.simple.JSONArray;
import org.json.simple.parser.*;
import org.json.simple.JSONObject;
import scala.Tuple2;

/**
 * DS-256 Assignment 0
 * Code for generating frequency distribution per hashtag
 */
public class FreqTag {

    public static void main(String[] args) throws ParseException {

        String inputFile = args[0]; // Should be some file on HDFS
        String outputFile = args[1]; // Should be some file on HDFS

        SparkConf sparkConf = new SparkConf().setAppName("FreqTag");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);

        /*
         * Code goes here
         */

        // Open file
        JavaRDD<String> twitterData = sc.textFile(inputFile);

        System.out.println("Shriram: File Opened !");

        // Convert to json
        JavaRDD<JSONObject> parsedData = twitterData.map((Function<String, JSONObject>) x -> (JSONObject) new JSONParser().parse(x));

        System.out.println("Shriram: Parsed JSON !");

        // Get Hash Count for each tweet
        JavaPairRDD<Integer, Integer> hashCount = parsedData.mapToPair(
                (PairFunction<JSONObject, Integer, Integer>) x -> {
                    try {
                        return new Tuple2(((JSONObject) x.get("user")).get("id"),
                                ((JSONArray) ((JSONObject) x.get("entities")).get("hashtags")).size());
                    } catch (Exception e) {
                        return new Tuple2(0, -1);
                    }
                });

        System.out.println("Shriram: Obtained Count !");

        // Filter invalid rows
        JavaPairRDD<Integer, Integer> validHashCount = hashCount.filter(x -> !(x._2.equals(-1)));

        System.out.println("Shriram: Filtered value !");

        // Get average per user

        JavaPairRDD<Integer, Double> avgPerUser = validHashCount.groupByKey().mapToPair(x -> {
            Integer c = 0;
            Double s = 0.0;
            while (x._2.iterator().hasNext()) {
                c++;
                s += x._2.iterator().next();
            }
            return new Tuple2(x._1, s/c);
        });


        System.out.println("Shriram: Obtained Avg. value !");

        // Save file
        avgPerUser.saveAsTextFile(outputFile);

        System.out.println("Shriram: Saved Output !");

        sc.stop();
        sc.close();

//        String s = "{\"created_at\":\"Wed Oct 19 06:27:50 +0000 2016\",\"id\":788627701406044160,\"id_str\":\"788627701406044160\",\"text\":\"Digrepein sama siapa aja mau\\ud83d\\ude0c\\ud83d\\udc4c https:\\/\\/t.co\\/YDXrYo3As9\",\"display_text_range\":[0,30],\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.c\n" +
//                "om\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"in_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"i\n" +
//                "d\":3325571352,\"id_str\":\"3325571352\",\"name\":\"\\ub0ad\\ub2c8 \\u82b1\",\"screen_name\":\"sneunx\",\"location\":\"ApinkeusqINFINITEAMSSQ\",\"url\":\"http:\\/\\/twitter.com\\/Apinksne\",\"description\":\"A-pink(\\uc5d0\\uc774\\ud551\\ud06c) visual (\\uc190\\ub098\\uc740) 4\\/6 pinkboo \\ud83c\\ud\n" +
//                "f52\\u2022 February 10, 1994 \\u2022 ijong's\\u2661 #fleugency #tinkerbellrps\",\"protected\":false,\"verified\":false,\"followers_count\":1319,\"friends_count\":1597,\"listed_count\":13,\"favourites_count\":780,\"statuses_count\":35478,\"created_at\":\"Sun Aug 23 04:11:28 +0000 20\n" +
//                "15\",\"utc_offset\":-25200,\"time_zone\":\"Pacific Time (US & Canada)\",\"geo_enabled\":false,\"lang\":\"id\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"000000\",\"profile_background_image_url\":\"http:\\/\\/abs.twimg.com\\/images\\/themes\\/theme\n" +
//                "1\\/bg.png\",\"profile_background_image_url_https\":\"https:\\/\\/abs.twimg.com\\/images\\/themes\\/theme1\\/bg.png\",\"profile_background_tile\":false,\"profile_link_color\":\"F58EA8\",\"profile_sidebar_border_color\":\"000000\",\"profile_sidebar_fill_color\":\"000000\",\"profile_text_c\n" +
//                "olor\":\"000000\",\"profile_use_background_image\":false,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/787665062580236296\\/ds_hE2g5_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/787665062580236296\\/ds_hE2g5_normal.jpg\n" +
//                "\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/3325571352\\/1476485421\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null,\"place\":null,\"contrib\n" +
//                "utors\":null,\"quoted_status_id\":788611777563156480,\"quoted_status_id_str\":\"788611777563156480\",\"quoted_status\":{\"created_at\":\"Wed Oct 19 05:24:34 +0000 2016\",\"id\":788611777563156480,\"id_str\":\"788611777563156480\",\"text\":\"digrepeinlah https:\\/\\/t.co\\/FLp3TDTPSo\",\"\n" +
//                "display_text_range\":[0,12],\"source\":\"\\u003ca href=\\\"http:\\/\\/twitter.com\\/download\\/android\\\" rel=\\\"nofollow\\\"\\u003eTwitter for Android\\u003c\\/a\\u003e\",\"truncated\":false,\"in_reply_to_status_id\":null,\"in_reply_to_status_id_str\":null,\"in_reply_to_user_id\":null,\"i\n" +
//                "n_reply_to_user_id_str\":null,\"in_reply_to_screen_name\":null,\"user\":{\"id\":2229529939,\"id_str\":\"2229529939\",\"name\":\"ijong \\u82b1\",\"screen_name\":\"Kkaijonx\",\"location\":null,\"url\":null,\"description\":\"Kim Jong In (KAI) failed duplicated || Lead Rapper \\u00a4 SM Enter\n" +
//                "tainment || are u simple people? lets be friends~ || #FLEUGENCY\",\"protected\":false,\"verified\":false,\"followers_count\":917,\"friends_count\":585,\"listed_count\":7,\"favourites_count\":166,\"statuses_count\":1653,\"created_at\":\"Wed Dec 04 08:20:43 +0000 2013\",\"utc_offset\n" +
//                "\":25200,\"time_zone\":\"Jakarta\",\"geo_enabled\":true,\"lang\":\"id\",\"contributors_enabled\":false,\"is_translator\":false,\"profile_background_color\":\"C6E2EE\",\"profile_background_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_background_images\\/523139178179989505\\/Vj_4Mm3A.j\n" +
//                "peg\",\"profile_background_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_background_images\\/523139178179989505\\/Vj_4Mm3A.jpeg\",\"profile_background_tile\":true,\"profile_link_color\":\"1F98C7\",\"profile_sidebar_border_color\":\"FFFFFF\",\"profile_sidebar_fill_color\":\"\n" +
//                "DAECF4\",\"profile_text_color\":\"663B12\",\"profile_use_background_image\":true,\"profile_image_url\":\"http:\\/\\/pbs.twimg.com\\/profile_images\\/787675678070022144\\/Km7Jzrfr_normal.jpg\",\"profile_image_url_https\":\"https:\\/\\/pbs.twimg.com\\/profile_images\\/78767567807002214\n" +
//                "4\\/Km7Jzrfr_normal.jpg\",\"profile_banner_url\":\"https:\\/\\/pbs.twimg.com\\/profile_banners\\/2229529939\\/1476463646\",\"default_profile\":false,\"default_profile_image\":false,\"following\":null,\"follow_request_sent\":null,\"notifications\":null},\"geo\":null,\"coordinates\":null\n" +
//                ",\"place\":null,\"contributors\":null,\"quoted_status_id\":788578021288534016,\"quoted_status_id_str\":\"788578021288534016\",\"is_quote_status\":true,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[\"abc\",\"def\",\"ghi\"],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/FLp3TDTPSo\n" +
//                "\",\"expanded_url\":\"https:\\/\\/twitter.com\\/sneunx\\/status\\/788578021288534016\",\"display_url\":\"twitter.com\\/sneunx\\/status\\/\\u2026\",\"indices\":[13,36]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"\n" +
//                "low\",\"lang\":\"in\"},\"is_quote_status\":true,\"retweet_count\":0,\"favorite_count\":0,\"entities\":{\"hashtags\":[\"abc\",\"def\",\"ghi\"],\"urls\":[{\"url\":\"https:\\/\\/t.co\\/YDXrYo3As9\",\"expanded_url\":\"https:\\/\\/twitter.com\\/Kkaijonx\\/status\\/788611777563156480\",\"display_url\":\"twitter.com\\/Kkaijonx\n" +
//                "\\/statu\\u2026\",\"indices\":[31,54]}],\"user_mentions\":[],\"symbols\":[]},\"favorited\":false,\"retweeted\":false,\"possibly_sensitive\":false,\"filter_level\":\"low\",\"lang\":\"in\",\"timestamp_ms\":\"1476858470666\"}";
//
//
//        JSONObject x = (JSONObject) new JSONParser().parse(s);
//
//        System.out.println("Here: ");
//        System.out.println(getHashCount(s).toString());

    }

//    private static Tuple2<Integer, Integer> getHashCount(JSONObject x) {
//
//        try {
//            return ((JSONArray)((JSONObject) x.get("entities")).get("hashtags")).size();
//        }
//        catch(Exception e) {
//            return 0;
//        }
//
//    }

}