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
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);

        List<Integer> test = new ArrayList<>();
        for(int i=0; i < 10000; i++) {
            test.add(i);
        }

        jsc.parallelize(test)
                .foreach(new VoidFunction<Integer>() {
                    @Override
                    public void call(Integer integer) throws Exception {
                        StringBuffer buffer = new StringBuffer();

                        try {
                            URL url = new URL("http://108.108.108.141:32438/");
                            HttpURLConnection httpUrlConn = (HttpURLConnection)url.openConnection();

                            httpUrlConn.setDoOutput(false);
                            httpUrlConn.setDoInput(true);
                            httpUrlConn.setUseCaches(false);

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

                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        System.out.println(buffer.toString());
                    }
                });

        jsc.stop();
    }
}
