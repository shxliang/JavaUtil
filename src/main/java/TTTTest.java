import com.alibaba.fastjson.JSONObject;
import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.NShort.NShortSegment;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

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
