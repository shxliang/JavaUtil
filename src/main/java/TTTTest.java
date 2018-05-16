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
        String text = "孙志刚强调当前要集中精力抓好灾后恢复重建各项工作把倒塌受损民房重建修缮作为重中之重妥善安排受灾群众基本生产生活";
        System.out.println(NShortSegment.parse(text));
    }
}
