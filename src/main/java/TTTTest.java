import com.alibaba.fastjson.JSONObject;

import java.io.IOException;

/**
 * @author lsx
 * @date 2018/4/16
 */
public class TTTTest {
    public static void main(String[] args) throws IOException {
        JSONObject jsonObject = new JSONObject();
        jsonObject.put(null, "test");
        jsonObject.put("", "11");

        System.out.println(jsonObject.keySet());
    }
}
