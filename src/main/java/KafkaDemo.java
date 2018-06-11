import com.alibaba.fastjson.JSONObject;
import kafka.KafkaUtil;

import java.util.Random;

/**
 * @author lsx
 * @date 2018/5/21
 */
public class KafkaDemo {
    public static void main(String[] args) throws InterruptedException {
        String jsonStr = "{\"postId\": \"995460997904297985\",\"created_at\": \"2018-05-13T00:29:31.000+0000\",\"source\": \"3\",\"clusterId\": \"995460997904297985\",\"uid\": \"991340155108777984\",\"type\": \"status\",\"ner\": [{\"keyword_content\": \"Urban America\",\"text_content\": \"Urban America\",\"type\": \"LOC\"}],\"friends\": [],\"twitterUser\": {\"screen_name\": \"realkareemdream\",\"name\": \"Kareem D. Lanier\",\"profile_image_url\": \"http://pbs.twimg.com/profile_images/991370347676164098/OtJ4LJcJ_normal.jpg\",\"lang\": \"en\",\"created_at\": \"2018-05-01T23:34:45.000+0800\",\"protected\": false,\"description\": \"Urban Revitalization Coalition | Working w/ White House’s DPC, NEC \\u0026 OAI to create “Urban Policies for Urban Revitalization to Restore Urban Pride!”\",\"statuses_count\": 12,\"friends_count\": 39,\"followers_count\": 7583,\"favourites_count\": 21,\"verified\": false,\"location\": \"United States\",\"url\": \"http://www.urcamerica.com\",\"listed_count\": 17},\"lived\": 0}";
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);
        Random random = new Random();
        while (true) {
            jsonObject.put("postId", String.valueOf(random.nextInt(10000)));
            KafkaUtil.send(System.currentTimeMillis(), jsonObject.toJSONString());
//            Thread.sleep(1);
        }
    }
}
