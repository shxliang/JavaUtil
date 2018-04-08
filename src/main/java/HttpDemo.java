import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;

/**
 * @author lsx
 */
public class HttpDemo {
    public static String doPost(String url, String body) throws Exception {
        String result = null;
        org.apache.commons.httpclient.HttpClient client = new org.apache.commons.httpclient.HttpClient();
        PostMethod method = new PostMethod(url);
        if (null != body) {
            method.setRequestEntity(new StringRequestEntity(body, "application/json", "UTF-8"));
        }
        try {
            client.executeMethod(method);
            //打印返回的信息
            result = method.getResponseBodyAsString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        method.releaseConnection();
        return result;
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        sqlContext.udf().register("getPorn", new UDF3<String, String, String, String>() {
            @Override
            public String call(String s, String s2, String s3) throws Exception {
                JSONObject body = new JSONObject();
                body.put("docId", s);
                body.put("title", s2);
                body.put("content", s3);
                body.put("moduler", new String[]{"porn"});
                String result = "";
                int count = 0;
                while (count < 10)
                {
                    try {
                        String res = doPost("http://developback.ddp.dacube.com.cn/analyze/api/earthlywebsite/detection/accessToken", body.toJSONString());
                        JSONObject jsonObject = JSONObject.parseObject(res);
                        result = jsonObject.getJSONObject("data").getString("porn");
                        break;
                    } catch (Exception e) {
                        count++;
                    }
                }
                return result;
            }
        }, DataTypes.StringType);

        sqlContext.udf().register("getGamble", new UDF3<String, String, String, String>() {
            @Override
            public String call(String s, String s2, String s3) throws Exception {
                JSONObject body = new JSONObject();
                body.put("docId", s);
                body.put("title", s2);
                body.put("content", s3);
                body.put("moduler", new String[]{"gamble"});
                String result = "";
                int count = 0;
                while (count < 10)
                {
                    try {
                        String res = doPost("http://developback.ddp.dacube.com.cn/analyze/api/earthlywebsite/detection/accessToken", body.toJSONString());
                        JSONObject jsonObject = JSONObject.parseObject(res);
                        result = jsonObject.getJSONObject("data").getString("gamble");
                        break;
                    } catch (Exception e) {
                        count++;
                    }
                }
                return result;
            }
        }, DataTypes.StringType);





        DataFrame dataFrame = sqlContext.read()
//                .parquet("hdfs://90.90.90.5:8020/user/ddp/LDA涉黄测试/testData/yellowTest")
                .parquet("hdfs://90.90.90.5:8020/user/ddp/LDA涉黄测试/testData/gambleTest")
//                .parquet("C:\\Users\\lsx\\Downloads\\sh_test.parquet")
                .sample(false, 0.1)
                .repartition(10);

//        dataFrame = dataFrame.withColumn("porn",
//                functions.callUDF("getPorn",
//                        functions.col("docId"),
//                        functions.col("title"),
//                        functions.col("text")));

        dataFrame = dataFrame.withColumn("gamble",
                functions.callUDF("getGamble",
                        functions.col("docId"),
                        functions.col("title"),
                        functions.col("text")));

        dataFrame = dataFrame.cache();

        long TP = dataFrame.filter("gamble='涉赌' AND class='涉赌'").count();
        long FP = dataFrame.filter("gamble='涉赌' AND class='其他'").count();
        long TN = dataFrame.filter("gamble='其他' AND class='其他'").count();
        long FN = dataFrame.filter("gamble='其他' AND class='涉赌'").count();
        double recall = (double) TP / (TP + FN);
        double precision = (double) TP / (TP + FP);
        double f1 = 2 * recall * precision / (recall + precision);

        dataFrame.groupBy("class").count().show();

        System.out.println("TP = " + TP);
        System.out.println("FP = " + FP);
        System.out.println("TN = " + TN);
        System.out.println("FN = " + FN);
        System.out.println("Recall = " + recall);
        System.out.println("Precision = " + precision);
        System.out.println("F1 = " + f1);
    }
}
