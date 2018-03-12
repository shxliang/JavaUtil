import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SQLContext;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Created by lsx on 2017/9/30.
 */
public class TTest {
    public static void main(String[] args) {
        StringBuffer buffer = new StringBuffer();
        int tryNum = 0;

        while (tryNum < 5)
        {
            try {
                URL url = new URL("http://58.16.65.217:9095/cmsapp/bh/getDocument?docUrl="
                        + "http://www.qdn.go01802/t20180224_2122373.html");
                HttpURLConnection httpUrlConn = (HttpURLConnection) url.openConnection();

                httpUrlConn.setDoOutput(false);
                httpUrlConn.setDoInput(true);
                httpUrlConn.setUseCaches(false);
                httpUrlConn.setConnectTimeout(1000);
                httpUrlConn.setReadTimeout(1000);

                httpUrlConn.setRequestMethod("GET");
                httpUrlConn.connect();

                // 将返回的输入流转换成字符串
                InputStream inputStream = httpUrlConn.getInputStream();
                InputStreamReader inputStreamReader = new InputStreamReader(inputStream, "utf-8");
                BufferedReader bufferedReader = new BufferedReader(inputStreamReader);

                String str = null;
                while ((str = bufferedReader.readLine()) != null) {
                    buffer.append(str);
                }
                bufferedReader.close();
                inputStreamReader.close();
                // 释放资源
                inputStream.close();
                inputStream = null;
                httpUrlConn.disconnect();

                break;
            } catch (Exception e) {
                System.out.println(e.getStackTrace());
                tryNum ++;
            }
        }
        JSONObject jsonObject = JSONObject.parseObject(buffer.toString());
        if (!jsonObject.isEmpty())
        {
            System.out.println(jsonObject);
        }else
        {
            System.out.println("not find");
        }
    }
}
