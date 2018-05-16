package util;

import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.LinkedList;
import java.util.List;

/**
 *
 * @author lsx
 * @date 2018/4/4
 */
public class FileUtil {
    public static List<String> readLocalFile(String fileName) throws IOException {
        List<String> lines = new LinkedList<>();

        File file = new File(fileName);
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = reader.readLine()) != null) {
                lines.add(line);
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e1) {
                }
            }
        }

        return lines;
    }

    public static List<String> readHdfsFile(String fileName) throws IOException {
        List<String> result = new LinkedList<>();

        Configuration config = new Configuration();
        config.set("fs.default.name", "90.90.90.5");
        FileSystem fileSystem = FileSystem.get(config);

        String line = null;
        Path vocabFile = new Path(fileName);
        FSDataInputStream vocabInStream = fileSystem.open(vocabFile);
        BufferedReader vocabBufferedReader = new BufferedReader(new InputStreamReader(vocabInStream, "utf-8"));
        while ((line = vocabBufferedReader.readLine()) != null) {
            result.add(line);
        }
        vocabBufferedReader.close();
        vocabInStream.close();

        return result;
    }

    public static void writeLines(List<String> lines, String filePath) throws IOException {
        File file = new File(filePath);
        BufferedWriter bufferedWriter = null;

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(file));
            for (String str : lines) {
                JSONObject jsonObject = JSONObject.parseObject(str);
                bufferedWriter.write(jsonObject.toJSONString());
                bufferedWriter.newLine();
            }
            bufferedWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedWriter != null) {
                bufferedWriter.close();
            }
        }
    }
}
