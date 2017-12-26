package hotwords;

import java.util.*;

/**
 *
 * @author root
 * @date 2017/12/13
 */
public class WordFind {
    /**
     * 把一篇文章 利用信息熵，凝固度 转换成词
     * @param line 文章
     * @param threshold 阈值（这里是凝固度的阈值） 信息熵的阈值取的0.2
     * @param phrase 原有分好的词
    */
    public final static String STOP_WORDS = "的很了么呢是嘛个都也比还这于不与才上用就好在和对挺去后没说到";
    public static String getWords(String line,String phrase,Double threshold){
        //候选词集合
        Set<String> wordsSet = new LinkedHashSet<>();
        //词和右信息熵
        Map<String, String> rightMap = new HashMap<>();
        //词和左信息熵
        Map<String, String> leftMap = new HashMap<>();
        List<String> result = new LinkedList<String>();
        //替换掉标点之类的非汉字
        line = line.replaceAll("[\\p{P}+~$`^=|<>～｀＄＾＋＝｜＜＞￥×]", "")
                .replaceAll("["+ STOP_WORDS +"]","");
        line = line.replaceAll(" ","");
        String[] parts = line.split(" ");
        int dataLength = parts.length;
        for (int i=0;i<dataLength;i++) {
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
                        rightMap.put(charResult, s == curLength-2 ? value : curParts.charAt(s) + "," + value);
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
            for (String curWord:wordsSet){
                Double left=0.0;
                Double right=0.0;
                if (leftMap.get(curWord).equals("")||"".equals(rightMap.get(curWord))) {
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
                for (String word:ULWord){
                    int count = Collections.frequency(lList,word);
                    left+=-((double)count/lList.size())*Math.log((double)count/lList.size());
                }
                for (String word:RSet){
                    int count = Collections.frequency(RList, word);
                    right+=-((double)count/RList.size())*Math.log((double)count/RList.size());
                }
                double min = Math.min(left, right);
//                System.out.println(curWord+"====最小的信息熵===="+min);
                if (min>0.2) {
                    resultMap.put(curWord, min);
                }
            }
        List<String> readyList = new ArrayList<>();
        Set<String> resultSet = resultMap.keySet();
            int toltalSize = line.length();
            for (String curWord:resultSet){
                Double n=1.0;
                for (int k=0;k<curWord.length();k++){
                    String curpart = curWord.substring(0, k);
                    int count = line.compareTo(curpart);
                    n=n*count/toltalSize;
                }
                if (n>threshold){
                    result.add(curWord+":"+n);
                    readyList.add(curWord);
                }
            }
        StringBuilder combineStr = new StringBuilder();
        String[] phrases = phrase.split(" ");
        int length = phrases.length;
        for (int i=0;i<phrases.length-1;i++){
            String word = phrases[i] + phrases[i + 1];
            if (readyList.contains(word)){
                combineStr.append(word+" ");
                i++;
            }else {
                combineStr.append(phrases[i]+" ");

            }
        }
        return combineStr.append(phrases[length-1]).toString().trim();
    }

    public static void main(String[] args) {
        String text = "蓝瘦香菇很蓝瘦香菇，因为蓝瘦香菇很蓝瘦，所以很香菇";
        String segmented = "蓝瘦 香菇 很 蓝瘦 香菇 ， 因为 蓝瘦 香菇 很 蓝瘦 ， 所以 很 香菇";
        System.out.println(getWords(text, segmented, 0.5));
    }
}
