package report;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.types.DataTypes;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by lsx on 2017/5/23.
 */

public class DataGrandReport implements Serializable
{
    @SuppressWarnings("resource")
    public String post(String reqURL, Map<String, String> params)
            throws ClientProtocolException, IOException{
        String responseContent = "";

        HttpPost httpPost = new HttpPost(reqURL);
        if (params != null) {
            List nvps = new ArrayList();
            Set<Entry<String, String>> paramEntrys = params.entrySet();
            for (Entry<String, String> entry : paramEntrys) {
                nvps.add(new BasicNameValuePair(entry.getKey(), entry.getValue()));
            }
            httpPost.setEntity(new UrlEncodedFormEntity(nvps, "utf-8"));
        }

        httpPost.setHeader("User-Agent", "datagrand/datareport/java sdk v1.0.0");
        httpPost.setHeader("Content-Type","application/x-www-form-urlencoded");

        HttpClient httpClient = new DefaultHttpClient();
        HttpParams httpParams = httpClient.getParams();
        HttpConnectionParams.setSoTimeout(httpParams, 60*1000);
        HttpConnectionParams.setConnectionTimeout(httpParams, 60*1000);

        HttpResponse response = httpClient.execute(httpPost);
        StatusLine status = response.getStatusLine();
        if (status.getStatusCode() >= HttpStatus.SC_MULTIPLE_CHOICES) {
            System.out.printf(
                    "Did not receive successful HTTP response: status code = {}, status message = {}",
                    status.getStatusCode(), status.getReasonPhrase());
            httpPost.abort();
        }

        HttpEntity entity = response.getEntity();
        if (entity != null) {
            responseContent = EntityUtils.toString(entity, "utf-8");
            EntityUtils.consume(entity);
        } else {
            System.out.printf("Http entity is null! request url is {},response status is {}", reqURL, response.getStatusLine());
        }
        return responseContent;
    }

    public static void main(String[] args){

        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        final DataGrandReport obj = new DataGrandReport();

        DataFrame data = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header","true")
                .load("file:///E:\\data_sample_pred.csv");
        data.registerTempTable("data");


        sqlContext.udf().register("getKeywords", new UDF3<String, String, String, Iterable<String>>()
        {
            @Override
            public Iterable<String> call(String s, String s2, String s3) throws Exception
            {
                Map<String,String> params = new HashMap<String,String>();
                params.put("appid", "4499170");
                params.put("title", s2);
                params.put("textid", s);
                params.put("text", s3);
                String res = null;
                try {
                    res = obj.post("http://taggingapi.datagrand.com/tagging/gengyun", params);
//                    System.out.println(res);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                JsonParser parse =new JsonParser();
                JsonObject json = (JsonObject) parse.parse(res);
                JsonArray jsonArray = json.getAsJsonArray("tag_list");
                List<String> result = new ArrayList<String>();
                for (int i=0;i<jsonArray.size();i++)
                {
                    JsonObject curJson = jsonArray.get(i).getAsJsonObject();
//                    result.add(curJson.get("tag").getAsString()+":"+curJson.get("weight"));
                    result.add(curJson.get("tag").getAsString());
                }
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));


        sqlContext.udf().register("getSentiment", new UDF3<String, String, String, Iterable<Double>>()
        {
            @Override
            public Iterable<Double> call(String s, String s2, String s3) throws Exception
            {
                Map<String,String> params = new HashMap<String,String>();
                params.put("appid", "4499170");
                params.put("title", s2);
                params.put("textid", s);
                params.put("text", s3);
                String res = null;
                try {
                    res = obj.post("http://sentimentapi.datagrand.com/sentiment/gengyun", params);
//                    System.out.println(res);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                JsonParser parse =new JsonParser();
                JsonObject json = (JsonObject) parse.parse(res);
                JsonObject resultJson = json.get("result").getAsJsonObject();
                List<Double> result = new ArrayList<>();
                result.add(resultJson.get("positive").getAsDouble());
                result.add(resultJson.get("negative").getAsDouble());
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.DoubleType));


        sqlContext.udf().register("getCategories", new UDF3<String, String, String, Iterable<String>>()
        {
            @Override
            public Iterable<String> call(String s, String s2, String s3) throws Exception
            {
                Map<String,String> params = new HashMap<String,String>();
                params.put("appid", "4499170");
                params.put("title", s2);
                params.put("textid", s);
                params.put("text", s3);
                String res = null;
                try {
                    res = obj.post("http://classifyapi.datagrand.com/classify/gengyun", params);
//                    System.out.println(res);
                } catch (Exception e) {
                    e.printStackTrace();
                    return null;
                }

                JsonParser parse =new JsonParser();
                JsonObject json = (JsonObject) parse.parse(res);
                JsonArray jsonArray = json.getAsJsonArray("result");
                List<String> result = new ArrayList<String>();
                for (int i=0;i<jsonArray.size();i++)
                {
                    JsonArray curJasonArray = jsonArray.get(i).getAsJsonArray();
//                    result.add(curJson.get("tag").getAsString()+":"+curJson.get("weight"));
                    result.add(curJasonArray.get(0).getAsString()+":"+curJasonArray.get(1).getAsDouble());
                }
                return result;
            }
        },DataTypes.createArrayType(DataTypes.StringType));


        DataFrame result = sqlContext.sql("SELECT *," +
                "getKeywords(docId,title,text) AS keywords_datagrand," +
                "getSentiment(docId,title,text) AS sentiment_datagrand," +
                "getCategories(docId,title,text) AS docClass_datagrand " +
                "FROM data");

        result.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header","true")
                .save("file:///E:\\compare_result.csv");

        result.show();





//        Map<String,String> params = new HashMap<String,String>();
//        params.put("appid", "4499170");
//        params.put("title", "顶顶顶");
//        params.put("textid", "435386945382932");
//        params.put("text", "3m每天百分之1利息，60元起步，有需要可以联系我，Q49663537，或者关注百度贴吧，老马平台吧！");
//
//        String res = null;
//        try {
//            res = obj.post("http://sentimentapi.datagrand.com/sentiment/gengyun", params);
//            System.out.println(res);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }

    }
}
