import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.common.Term;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

import java.util.*;

/**
 * @author root
 * @date 2017/12/13
 */
public class NewWordFinder {
    /**
     * 把一篇文章 利用信息熵，凝固度 转换成词
     *
     * @param line 文章
     * @param threshold 阈值（这里是凝固度的阈值） 信息熵的阈值取的0.2
     * @param phrase 原有分好的词
     */
    public final static String stopwords = "的很了么呢是嘛个都也比还这于不与才上用就好在和对挺去后没说到";

    public static String getWords(String line, String phrase, Double threshold) {
        Set<String> wordsSet = new LinkedHashSet<>();//候选词集合
        Map<String, String> rightMap = new HashMap<>();//词和右信息熵
        Map<String, String> leftMap = new HashMap<>();//词和左信息熵
        List<String> result = new LinkedList<String>();
        line = line.replaceAll("[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]", "").replaceAll("[" + stopwords + "]", "");//替换掉标点之类的非汉字
        line = line.replaceAll(" ", "");
        String[] parts = line.split(" ");
        int dataLength = parts.length;
        for (int i = 0; i < dataLength; i++) {
            String curParts = parts[i];
            int curLength = curParts.length();
            if (curLength <= 2) {
                continue;
            }
            for (int j = 0; j < curLength - 1; j++) {
                for (int s = 2 + j; s < curLength; s++) {
                    String charResult = curParts.substring(j, s);
                    if (charResult.length() > 5) {
                        break;
                    }
                    wordsSet.add(charResult);

                    if (rightMap.containsKey(charResult)) {
                        String value = rightMap.get(charResult);
                        rightMap.put(charResult, s == curLength - 2 ? value : curParts.charAt(s) + "," + value);
                    } else {
                        rightMap.put(charResult, s == curLength ? "" : curParts.charAt(s) + "");
                    }
                    if (leftMap.containsKey(charResult)) {
                        String value = leftMap.get(charResult);
                        leftMap.put(charResult, j == 0 ? value : curParts.charAt(j - 1) + "," + value);
                    } else {
                        leftMap.put(charResult, j == 0 ? "" : curParts.charAt(j - 1) + "");
                    }
                }
            }
        }
        Map<String, Double> resultMap = new HashMap<>();
        for (String curWord : wordsSet) {
            Double left = 0.0;
            Double right = 0.0;
            if ("".equals(leftMap.get(curWord)) || "".equals(rightMap.get(curWord))) {
                continue;
            }
            String leftWords = leftMap.get(curWord);
            String rightWords = rightMap.get(curWord);
            String[] rpatrs = rightWords.split(",");
            List<String> RList = Arrays.asList(rpatrs);
            Set<String> RSet = new HashSet<>(RList);
            String[] LwordsParts = leftWords.split(",");
            List<String> lList = Arrays.asList(LwordsParts);
            Set<String> ULWord = new HashSet<>(lList);
            for (String word : ULWord) {
                int count = Collections.frequency(lList, word);
                left += -((double) count / lList.size()) * Math.log((double) count / lList.size());
            }
            for (String word : RSet) {
                int count = Collections.frequency(RList, word);
                right += -((double) count / RList.size()) * Math.log((double) count / RList.size());
            }
            double min = Math.min(left, right);
//                System.out.println(curWord+"====最小的信息熵===="+min);
            if (min > 0.2) {
                resultMap.put(curWord, min);
            }
        }
        List<String> readyList = new ArrayList<>();
        Set<String> resultSet = resultMap.keySet();
        int toltalSize = line.length();
        for (String curWord : resultSet) {
            Double n = 1.0;
            for (int k = 0; k < curWord.length(); k++) {
                String curpart = curWord.substring(0, k);
                int count = line.compareTo(curpart);
                n = n * count / toltalSize;
            }
            if (n > threshold) {
                result.add(curWord + ":" + n);
                readyList.add(curWord);
            }
        }
        StringBuilder combineStr = new StringBuilder();
        String[] phrases = phrase.split(" ");
        int length = phrases.length;
        for (int i = 0; i < phrases.length - 1; i++) {
            String word = phrases[i] + phrases[i + 1];
            if (readyList.contains(word)) {
                combineStr.append(word + " ");
                i++;
            } else {
                combineStr.append(phrases[i] + " ");

            }
        }
        return combineStr.append(phrases[length - 1]).toString().trim();
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "d:\\winutils");
        SparkConf sc = new SparkConf()
                .setMaster("local").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);
        String trainDataPath = "hdfs://90.90.90.5:8020/user/ddp/新媒体数据/mdopTrainData/par.parquet";
        DataFrame data = sqlContext.read().parquet(trainDataPath).limit(1);
        sqlContext.udf().register("combword", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                String words = getWords(s2, s2, 0.5);
                return words;
            }
        }, DataTypes.StringType);
        sqlContext.udf().register("HanLp", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                StringBuilder result = new StringBuilder();
                List<Term> segment = HanLP.segment(s);
                for (Term term : segment) {
                    result.append(term.word + " ");
                }
                return result.toString().trim();
            }
        }, DataTypes.StringType);
        data.registerTempTable("data");

        sqlContext.sql("SELECT HanLp(text) AS segmentWords FROM data").show(false);
        sqlContext.sql("select combword(segmentedWords,segmentedWords) AS words from data ").show(false);
    }
}
