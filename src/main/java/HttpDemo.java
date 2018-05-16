import com.alibaba.fastjson.JSONObject;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.URI;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import util.FileUtil;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

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
            result = method.getResponseBodyAsString();
        } catch (IOException e) {
            e.printStackTrace();
        }

        method.releaseConnection();
        return result;
    }

    public static String doGet(String url) throws Exception {
        String result = "";
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);
        try {
            client.executeMethod(method);
            //打印返回的信息
            InputStream inputStream = method.getResponseBodyAsStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = br.readLine();
            while (null != line) {
                result += line + "\r\n";
                line = br.readLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            //释放连接
            method.releaseConnection();
        }
        return result;
    }

    public static String setGet(String url) {
        HttpClient httpClient = new HttpClient();
        GetMethod getMethod = new GetMethod();
        String responseMsg = null;

        try {
            getMethod.setURI(new URI(url));
            httpClient.executeMethod(getMethod);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            InputStream in = getMethod.getResponseBodyAsStream();
            int len = 0;
            byte[] buf = new byte[1024];
            while ((len = in.read(buf)) != -1) {
                out.write(buf, 0, len);
            }
            responseMsg = out.toString("UTF-8");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            getMethod.releaseConnection();
        }
        return responseMsg;
    }


    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\winutils");


//        String jsonStr = "{\"from\":0,\"size\": 10000,\"_source\": [\"docId\",\"title\",\"text\",\"sourceName\",\"publishTime\"],\"query\": {\"bool\": {\"must\": [{\"nested\": {\"path\": \"categories\",\"query\": {\"bool\":{\"must\": [{\"match\": {\"categories.categoryId\": {\"query\": \"m1iKDW6VHg-1485066535884-8xZbK2Gjgt\",\"operator\": \"and\"}}}]}}}},{\"range\": {\"publishTime\": {\"gte\": \"2018-05-01 00:00:00\",\"lte\":\"2018-05-11 00:00:00\"}}}]}},\"sort\": [{\"publishTime\": {\"order\": \"desc\"}}]}";
//        JSONObject body = JSONObject.parseObject(jsonStr);
//        String resStr = doPost("http://60.60.60.20:10090/api/cms_data/homepage/getArticlesByQueryString", body.toJSONString());
//        JSONObject resJson = JSONObject.parseObject(resStr);
//        JSONArray jsonArray = resJson.getJSONArray("data");


//        String[] siteArray = new String[]{
//                "C98p4Sf6R9EchfLKUhQUmBMMYp0cR4jq",
//                "qlIKuy0b50S0Kx1uvHKzLx4xrPD5FB7z",
//                "v9sjeVhz2lv0MqcMe0DslJNIaQfaLRzP",
//                "FnkAYhGo8aPOr9ecUYstGYQpBiJDofiJ",
//                "talARQqDjfQigseyWpM88qyDJ69MiYMu"
//        };
//        List<String> result = new LinkedList<>();
//        String apiStr = "http://60.60.60.20:10080/api/readHbase/";
//        int dataSize = 100;
//
//        for (String curSite : siteArray) {
//            int count = 0;
//
//            String resStr = setGet(apiStr + curSite + "/" + dataSize);
//            JSONObject resJson = JSONObject.parseObject(resStr);
//            JSONArray dataArray = resJson.getJSONArray("data");
//
//            while (dataArray.size() == dataSize && count <= 10000) {
//                count += dataArray.size();
//                for (int i = 0; i < dataArray.size(); i++) {
//                    result.add(dataArray.getJSONObject(i).toJSONString());
//                }
//
//                System.out.println(count);
//
//                String nextRow = resJson.getString("nextRow");
//                resStr = setGet(apiStr + nextRow + "/" + dataSize);
//                resJson = JSONObject.parseObject(resStr);
//                dataArray = resJson.getJSONArray("data");
//            }
//        }


        String apiStr = "http://120.26.94.161:10075/articleParser/getCaseSeeksInfo";
        List<String> result = new LinkedList<>();

        List<String> lines = FileUtil.readLocalFile("D:\\\\Downloads\\\\yuqingtong.json");
        int totalCount = lines.size();
        int curCount = 0;

        for (String line : lines) {
            curCount++;
            if (curCount % 50 == 0) {
                System.out.println(curCount + " / " + totalCount);
            }

            JSONObject lineJson = JSONObject.parseObject(line);
            String title = lineJson.getString("title");
            String url = lineJson.getString("url");
            String media = lineJson.getString("media");
            String body = "{\"url\":\"" + url + "\"}";
            String content = "";

            int numTry = 0;
            while (numTry < 5) {
                try {
                    String resStr = doPost(apiStr, body);
                    JSONObject resJson = JSONObject.parseObject(resStr);
                    content = resJson.getJSONObject("context").getString("content");
                    break;
                } catch (Exception e) {
                    System.out.println(body);
                    numTry++;
                }
            }

            JSONObject jsonObject = new JSONObject();
            jsonObject.put("url", url);
            jsonObject.put("media", media);
            jsonObject.put("title", title);
            jsonObject.put("content", content);
            result.add(jsonObject.toJSONString());
        }

        FileUtil.writeLines(result, "D:\\Downloads\\yuqingtong_result.json");
    }
}
