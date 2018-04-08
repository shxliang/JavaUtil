package fbparser;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.util.PDFTextStripper;
import util.KMP;

import java.io.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author lsx
 * @date 2018/3/15
 */
public class PDFParser {
    private static Pattern titlePattern = Pattern.compile("^(\\d+\\.)+");
    private static Pattern numPattern = Pattern.compile("^\\d+");
    private static Pattern CnPattern = Pattern.compile("C(\\d)");
    private static List<String> typeList = new LinkedList<>();

    static {
        typeList.add("C");
        typeList.add("F");
        typeList.add("D");
        typeList.add("DT");
        typeList.add("QX");
        typeList.add("N");
        typeList.add("M");
        typeList.add("Cdx");
        typeList.add("Cgl");
        typeList.add("Cpc");
        typeList.add("BB");
        typeList.add("RID");
        typeList.add("CID");
    }

    public static void pdf2text(String inputPath, String outputPath) {
        try {
            // 是否排序
            boolean sort = false;
            // 开始提取页数
            int startPage = 1;
            // 结束提取页数
            int endPage = Integer.MAX_VALUE;
            String content = null;
            PrintWriter writer = null;
            PDDocument document = PDDocument.load(new File(inputPath));
            PDFTextStripper pts = new PDFTextStripper();
            endPage = document.getNumberOfPages();
            System.out.println("Total Page: " + endPage);
            pts.setStartPage(startPage);
            pts.setEndPage(endPage);

            try {
                //content就是从pdf中解析出来的文本
                content = pts.getText(document);
                writer = new PrintWriter(new FileOutputStream(outputPath));
                // 写入文件内容
                writer.write(content);
                writer.flush();
                writer.close();
            } catch (Exception e) {
                throw e;
            } finally {
                if (null != document) {
                    document.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static List<String> readFromTxt(String inputPath) throws IOException {
        List<String> paragraph = new LinkedList<>();
        File filename = new File(inputPath);
        // 建立一个输入流对象reader
        InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
        BufferedReader br = new BufferedReader(reader);
        String line;
        while ((line = br.readLine()) != null) {
            paragraph.add(line);
        }
        return paragraph;
    }

    public static List<String> getTitles(String inputPath) {
        List<String> titles = new LinkedList<>();
        try {
            File filename = new File(inputPath);
            // 建立一个输入流对象reader
            InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
            BufferedReader br = new BufferedReader(reader);
            String line;
            line = br.readLine();
            titles.add(line + "--" + 1);
            while ((line = br.readLine()) != null) {
                Matcher titleMatcher = titlePattern.matcher(line);
                if (titleMatcher.find()) {
                    int level = KMP.search(".", line).size() + 1;
                    titles.add(line + "--" + level);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return titles;
    }

    public static List<String> paragraphFilter(List<String> paragraphs, List<String> titles, String code) {
        int paraIndex = 0;
        int titleIndex = 0;
        List<String> parsedParagraph = new LinkedList<>();
        while (paraIndex < paragraphs.size()) {
            String para = paragraphs.get(paraIndex);

            if (titleIndex == titles.size()) {
                if (para.trim().contains("见表")
                        || para.startsWith("表 ")
                        || para.startsWith("序号 ")
                        || para.startsWith("代 码")
                        || "是".equals(para.trim())) {
                    paraIndex++;
                } else if (para.trim().equals(code)) {
                    paraIndex += 2;
                } else {
                    parsedParagraph.add(para);
                    paraIndex++;
                }
                continue;
            }

            String title = titles.get(titleIndex);
            String[] titleParts = title.split("--");

            //当前行为标题
            if (para.trim().equals(titleParts[0].trim())) {
                parsedParagraph.add(title);
                paraIndex++;
                titleIndex++;
            }
            //当前行为内容
            else if (para.trim().length() > 0) {
                if (para.trim().contains("见表")
                        || para.startsWith("表")
                        || para.startsWith("序号")
                        || para.startsWith("代 码")) {
                    paraIndex++;
                } else if (para.trim().equals(code)) {
                    paraIndex += 2;
                } else {
                    parsedParagraph.add(para);
                    paraIndex++;
                }
            } else {
                paraIndex++;
            }
        }
        return parsedParagraph;
    }

    public static Object recursiveParse(List<String> paragraph, int level) {
        //存储相同层级的para下标
        List<Integer> index = new LinkedList<>();
        JSONObject result = new JSONObject();

        for (int i = 0; i < paragraph.size(); i++) {
            String para = paragraph.get(i);

            //找到与传入层级相同的标题
            if (para.contains("--" + level)) {
                index.add(i);
            }
        }

        //当前块没有找到标题
        if (index.size() < 1) {
            return parseParagraph2JSONArray(paragraph);
        } else if (index.size() > 0) {
            for (int i = 0; i < index.size(); i++) {
                //得到标题
                String key = paragraph.get(index.get(i)).split("--")[0].split(" ")[1].trim();
                if (key.contains("采集的案件范围"))
                {
                    continue;
                }
                //得到当前标题下的内容块
                List<String> subParagraph = paragraph.subList(index.get(i) + 1,
                        (i + 1) < index.size() ? index.get(i + 1) : paragraph.size());
                result.put(key, recursiveParse(subParagraph, level + 1));
            }
        }

        return result;
    }

    public static JSONArray parseParagraph2JSONArray(List<String> paragraph) {
//        System.out.println(paragraph);
        JSONArray result = new JSONArray();
        List<Integer> lastNumIndexList = new LinkedList<>();
        List<Integer> typeIndexList = new LinkedList<>();
        List<String> elementList = new LinkedList<>();
        int curIndex = -1;
        int lastNumIndex = -1;

        for (String para : paragraph) {
            String[] parts = para.trim().split(" +");
            if (parts.length < 1) {
                continue;
            }
            for (String part : parts) {
                if (part.trim().length() < 1) {
                    continue;
                }

                curIndex++;
                elementList.add(part.trim());

                //匹配是否为数字
                Matcher numMatcher = numPattern.matcher(part.trim());
                if (numMatcher.find()) {
                    lastNumIndex = curIndex;
                } else {
                    //匹配是否为Cn类型
                    Matcher CnMatcher = CnPattern.matcher(part.trim());
                    //判断是否为类型
                    if (typeList.contains(part.trim()) || CnMatcher.find()) {
                        typeIndexList.add(curIndex);
                        lastNumIndexList.add(lastNumIndex);
                    }
                }
            }
        }

        for (int i = 0; i < lastNumIndexList.size(); i++) {
            JSONObject curJson = new JSONObject();
            curJson.put("name", parse2Name(
                    elementList.subList(lastNumIndexList.get(i) + 1,
                            typeIndexList.get(i))));
            curJson.put("type", elementList.get(typeIndexList.get(i)));
            int curLastNumIndex = (i + 1) < lastNumIndexList.size()
                    ? lastNumIndexList.get(i + 1) : elementList.size();
            if (curLastNumIndex - typeIndexList.get(i) - 1 > 0)
            {
                Map<String, String> enumMap = parse2Enum(
                        elementList.subList(typeIndexList.get(i) + 1,
                                curLastNumIndex));
                curJson.put("enum", enumMap);
            }
            result.add(curJson);
        }

        if (result.size() < 1)
        {
            System.out.println("Find error!");
        }

        return result;
    }

    public static String parse2Name(List<String> paragraph) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String para : paragraph) {
            stringBuilder.append(para.trim());
        }
        return stringBuilder.toString();
    }

    public static Map<String, String> parse2Enum(List<String> paragraph)
    {
        System.out.println(paragraph);
        Map<String, String> enumMap = new HashMap<>();
        List<String> cacheList = new LinkedList<>();
        String curKey = null;
        for(int i = 0; i < paragraph.size(); i++)
        {
            String[] parts = paragraph.get(i).trim().split(" +");
            if (parts.length < 1)
            {
                continue;
            }
            for (String part : parts)
            {
                if (part.trim().length() < 1)
                {
                    continue;
                }
                Matcher numMatcher = numPattern.matcher(part.trim());
                if (numMatcher.find())
                {
                    if (cacheList.size() > 0 && curKey != null)
                    {
                        StringBuilder stringBuilder = new StringBuilder();
                        for(String cache : cacheList)
                        {
                            stringBuilder.append(cache);
                        }
                        enumMap.put(curKey, stringBuilder.toString());
                    }

                    curKey = part.trim();
                    cacheList.clear();
                }
                else
                {
                    cacheList.add(part.trim());
                }
            }
        }
        if (cacheList.size() > 0 && curKey != null)
        {
            StringBuilder stringBuilder = new StringBuilder();
            for(String cache : cacheList)
            {
                stringBuilder.append(cache);
            }
            enumMap.put(curKey, stringBuilder.toString());
        }
        return enumMap;
    }

    public static JSONObject parseCaseTypeCode(String filePath) throws IOException {
        JSONObject result = new JSONObject();
        File filename = new File(filePath);
        // 建立一个输入流对象reader
        InputStreamReader reader = new InputStreamReader(new FileInputStream(filename));
        BufferedReader br = new BufferedReader(reader);
        String line;
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("\t");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("code", parts[0]);
            if (parts.length == 3)
            {
                jsonObject.put("abb", parts[2]);
            }
            result.put(parts[1], jsonObject);
        }
        return result;
    }

    public static JSONObject parseCourtCode(List<String> paragraph)
    {
        JSONObject result = new JSONObject();
        for(String para : paragraph)
        {
            if (para.contains("--") || para.trim().length() < 1)
            {
                continue;
            }
            String[] parts = para.trim().split(" +");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("value", parts[3]);
            jsonObject.put("abb", parts[4] + (parts.length == 6 ? parts[5] : ""));
            result.put(parts[0], jsonObject);
        }
        return result;
    }

    public static JSONObject parseCauseCode(List<String> paragraph)
    {
        JSONObject result = new JSONObject();
        for(String para : paragraph)
        {
            if (para.trim().length() < 1)
            {
                continue;
            }
            String[] parts = para.trim().split(" +");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("value", parts[3]);
            jsonObject.put("parentCode", parts[1]);
            jsonObject.put("gradeCode", parts[2]);
            result.put(parts[0], jsonObject);
        }
        return result;
    }

    public static JSONObject parseCauseCode2(List<String> paragraph)
    {
        JSONObject result = new JSONObject();
        for(String para : paragraph)
        {
            if (para.trim().length() < 1)
            {
                continue;
            }
            String[] parts = para.trim().split(" +");
            JSONObject jsonObject = new JSONObject();
            jsonObject.put("value", parts[1]);
            result.put(parts[0], jsonObject);
        }
        return result;
    }

    public static void main(String[] args) throws Exception {
        String inputPath = "fb/19.FYB_T_51201-2016_案件类型代码技术规范.pdf";
        String textOutputPath = inputPath.replaceAll(".pdf", ".txt");
        String filterOutputPath = textOutputPath.replace(".txt", "") + "_filter.txt";

//        pdf2text(inputPath, textOutputPath);

//        List<String> paragraph = readFromTxt(textOutputPath);
//        List<String> titles = getTitles(textOutputPath);
//        List<String> parsedParagraph = paragraphFilter(paragraph, titles, "FYB/T 51202—2016");
//
//        FileOutputStream outSTr = new FileOutputStream(new File(filterOutputPath));
//        BufferedOutputStream buff = new BufferedOutputStream(outSTr);
//        for (String para : parsedParagraph) {
//            buff.write((para + "\n").getBytes());
//        }
//        buff.flush();
//        buff.close();

//        List<String> parsedParagraph = readFromTxt(filterOutputPath);
//        JSONObject result = (JSONObject)recursiveParse(parsedParagraph, 1);
//        System.out.println(result);


        //转换案件类型代码
        System.out.println(parseCaseTypeCode("fb/19.FYB_T_51201-2016_案件类型代码技术规范.txt"));


        //转换法院代码
//        List<String> parsedParagraph = readFromTxt(filterOutputPath);
//        JSONObject result = parseCourtCode(parsedParagraph);
//        System.out.println(result);


        //转换案由代码
//        List<String> parsedParagraph = readFromTxt(textOutputPath);
//        JSONObject result = parseCauseCode2(parsedParagraph);
//        System.out.println(result);

    }
}
