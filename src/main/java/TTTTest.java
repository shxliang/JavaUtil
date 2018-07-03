import com.alibaba.fastjson.JSONObject;
import util.KMPSearch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lsx
 * @date 2018/4/16
 */
public class TTTTest {
    public static void main(String[] args) throws IOException {
//        JSONObject jsonObject = new JSONObject();
//        jsonObject.put("a", "a");
//        jsonObject.put("b", "b");
//        List<JSONObject> l = new LinkedList<>();
//        JSONObject j = new JSONObject();
//        j.put("t", "t");
//        l.add(j);
//        jsonObject

        List<Integer> l = KMPSearch.search("asd","c");
        System.out.println(l);

//        System.out.println(jsonObject.toJSONString());
    }
}
