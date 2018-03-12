import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSONObject;
import org.apache.poi.POIXMLDocument;
import org.apache.poi.POIXMLTextExtractor;
import org.apache.poi.hwpf.extractor.WordExtractor;
import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.xwpf.extractor.XWPFWordExtractor;
import org.apache.xmlbeans.XmlException;
import scala.Tuple2;

/**
 * @author lsx
 */
public class Docx2Text {
    private static Pattern sepPattern = Pattern.compile("[ \t]");
    private static Pattern numPattern = Pattern.compile("\\d+");
    private static Pattern numKeyPatternAfter = Pattern.compile("\\d+[ ]?");
    private static Pattern numKeyPatternBefore = Pattern.compile("[\t]\\d+");


    /**
     * 读取word文件内容 
     *
     * @param path
     * @return buffer
     */
    public String readWord(String path) {
        String buffer = "";
        try {
            if (path.endsWith(".doc")) {
                InputStream is = new FileInputStream(new File(path));
                WordExtractor ex = new WordExtractor(is);
                buffer = ex.getText();
                ex.close();
            } else if (path.endsWith("docx")) {
                OPCPackage opcPackage = POIXMLDocument.openPackage(path);
                POIXMLTextExtractor extractor = new XWPFWordExtractor(opcPackage);
                buffer = extractor.getText();
                extractor.close();
            } else {
                System.out.println("此文件不是word文件！");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return buffer;
    }

    public static List<String> parseParagraph(String[] paragraphs, List<String> titles)
    {
        int paraIndex = 0;
        int titleIndex = 0;
        List<String> parsedParagraph = new ArrayList<>();
        while (paraIndex < paragraphs.length) {
            String para = paragraphs[paraIndex];

            if (titleIndex == titles.size())
            {
                parsedParagraph.add(para);
                paraIndex++;
                continue;
            }

            String title = titles.get(titleIndex);
            String[] titleParts = title.split("-");
            Matcher afterMatcher = numKeyPatternAfter.matcher(para);
            Matcher beforeMatcher = numKeyPatternBefore.matcher(para);

            if (para.equals(titleParts[0])) {
                parsedParagraph.add(title);
                paraIndex++;
                titleIndex++;
            } else if (afterMatcher.find() || beforeMatcher.find()) {
                parsedParagraph.add(para);
                paraIndex++;
            } else {
                paraIndex++;
            }
        }
        return parsedParagraph;
    }

    public static Map<String, String> parseToMap(String str)
    {
        Map<String, String> result = new HashMap<>();
        int start = 0;
        String curStr = str;
        String key = null;
        String value = null;
        while (curStr.length() > 0)
        {
            Matcher beforeMatcher = numKeyPatternBefore.matcher(curStr);

            if (beforeMatcher.find())
            {

                start = beforeMatcher.start();

                if (start > 0)
                {
                    value = curStr.substring(0, start).trim();
                    result.put(key, value);
                }

                key = beforeMatcher.group().trim();
                curStr = curStr.substring(beforeMatcher.group().length());

            }
            else
            {
                value = curStr.trim();
                result.put(key, value);
                break;
            }
        }
        return result;
    }

    public static JSONObject recusiveParse(List<String> paragraph, int level)
    {
        List<Integer> index = new ArrayList<>();
        Map<String, Map<String, String>> map = new HashMap<>();
        String curKey = null;
        JSONObject result = new JSONObject();

        for(int i = 0; i < paragraph.size(); i++)
        {
            String para = paragraph.get(i);
            if (para.contains("-" + level))
            {
                index.add(i);
                continue;
            }

            Matcher afterMatcher = numKeyPatternAfter.matcher(para);
            Matcher beforeMatcher = numKeyPatternBefore.matcher(para);
            if (afterMatcher.find() || beforeMatcher.find())
            {
                int afterFirst = Integer.MAX_VALUE;
                int beforeFirst = Integer.MAX_VALUE;

                if (afterMatcher.find())
                {
                    afterFirst = afterMatcher.start();
                }
                if (beforeMatcher.find())
                {
                    beforeFirst = beforeMatcher.start();
                }

                int first = afterFirst < beforeFirst ? afterFirst : beforeFirst;

                String key = null;

                try {
                    key = para.substring(0, first).trim();
                }catch (Exception e)
                {
                    System.out.println(afterMatcher.find());
                    System.out.println(beforeMatcher.find());
                    System.out.println(afterFirst);
                    System.out.println(beforeFirst);
                    System.out.println(para);
                }

                Matcher numMatcher = numPattern.matcher(key);
                if (!numMatcher.matches())
                {
                    curKey = key;
                    String keyValueStr = para.substring(first, para.length());
                    map.put(curKey, parseToMap(keyValueStr));
                }
                else
                {
                    map.get(curKey).putAll(parseToMap(para));
                }
            }
        }

        if (index.size() < 1 && map.size() > 0)
        {
            for (String key : map.keySet())
            {
                result.put(key, map.get(key));
            }
        }
        else if(index.size() > 0)
        {
            for(int i = 0; i < index.size(); i++)
            {
                String key = paragraph.get(index.get(i)).split("-")[0];
                List<String> subParagraph = paragraph.subList(index.get(i),
                        (i + 1) < index.size() ? index.get(i + 1) : paragraph.size());
                result.put(key, recusiveParse(subParagraph, level + 1));
            }
        }

        return result;
    }



    public static void main(String[] args) throws OpenXML4JException, XmlException, IOException {
        Docx2Text tp = new Docx2Text();
        String path = "D:\\分析项目\\智慧法院\\法院标准\\\\2015人民法院案件信息业务标准\\15法标.docx";
        String content = tp.readWord(path);
        List<Tuple2<String, String>> tuple2List= new ArrayList<>();
        Tuple2<String, String> curTup = null;
        String[] paragraphs = content.split("\n");
        List<String> titles = WordUtil.getWordTitles2007(path);

        List<String> parsedParagraph = parseParagraph(paragraphs, titles);

        JSONObject result = recusiveParse(parsedParagraph, 1);

        System.out.println(result);
    }
}  