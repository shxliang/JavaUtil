import com.alibaba.fastjson.JSONObject;
import redis.RedisClient;

import java.util.List;

/**
 * @author lsx
 * @date 2018/5/2
 */
public class TTTest {
    public static void main(String[] args) {
        List<String> news = RedisClient.lrange("TTyIFTXf9iu9wPT2LgK0jKQY0focMWw0::records");
        int count = 0;
        for (String str : news) {
            JSONObject jsonObject = JSONObject.parseObject(str);
            JSONObject parsedJson = JSONObject.parseObject(jsonObject.getString("parsedData"));
            if (parsedJson.getString("keywords").trim().length() > 0) {
                count++;
                System.out.println("title: " + parsedJson.getString("title"));
                System.out.println("keywords: " + parsedJson.getString("keywords"));
                System.out.println("url: " + parsedJson.getString("url"));
            }
        }
        System.out.println(count);
    }
}
