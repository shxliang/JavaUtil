import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 * @author lsx
 * @date 2018/4/16
 */
public class TTTTest {
    public static void main(String[] args) throws IOException {
//        File leaderNameFile = new File("D:\\分析项目\\错别字\\3\\leaderName.txt");
//        BufferedReader leaderNameReader = new BufferedReader(new FileReader(leaderNameFile));
//        String leaderNameLine = null;
//        List<String> leaderNameList = new LinkedList<>();
//        while ((leaderNameLine = leaderNameReader.readLine()) != null) {
//            leaderNameList.add(leaderNameLine);
//        }
//        leaderNameReader.close();




        File file = new File("D:\\datas\\cnews\\cnews.test.txt");
        BufferedReader reader = new BufferedReader(new FileReader(file));
        String line = null;
        List<String> lines = new LinkedList<>();
        int id = 0;
        while ((line = reader.readLine()) != null) {
            JSONObject jsonObject = new JSONObject();
            String[] parts = line.split("\t");
            if (!"时政".equals(parts[0])) {
                continue;
            }
            jsonObject.put("id", String.valueOf(id++));
            jsonObject.put("text", parts[1]);
            lines.add(jsonObject.toJSONString());
        }
        reader.close();


        FileWriter fileWriter = new FileWriter("D:\\Downloads\\cnews_test.txt");
        BufferedWriter writer = new BufferedWriter(fileWriter);
        for (String str : lines) {
            writer.write(str);
            writer.write("\n");
        }
        writer.close();
        fileWriter.close();
    }
}
