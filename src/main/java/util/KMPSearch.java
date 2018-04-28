package util;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author lsx
 * @date 2017/11/6
 */
public class KMPSearch {
    private static int[] getNext(String b)
    {
        int len=b.length();
        int j=0;

        //next表示长度为i的字符串前缀和后缀的最长公共部分，从1开始
        int[] next = new int[len+1];
        next[0]=next[1]=0;

        //i表示字符串的下标，从0开始
        for(int i=1;i<len;i++)
        {//j在每次循环开始都表示next[i]的值，同时也表示需要比较的下一个位置
            while(j>0&&b.charAt(i)!=b.charAt(j)) {
                j = next[j];
            }
            if(b.charAt(i)==b.charAt(j)) {
                j++;
            }
            next[i+1]=j;
        }

        return next;
    }

    public static List<Integer> search(String original, String find) {
        List<Integer> result = new ArrayList<>();
        int[] next = getNext(original);
        int j = 0;
        for (int i = 0; i < original.length(); i++)
        {
            while (j > 0 && original.charAt(i) != find.charAt(j)) {
                j = next[j];
            }
            if (original.charAt(i) == find.charAt(j)) {
                j++;
            }
            if (j == find.length())
            {
                result.add(i - j + 1);
                j = next[j];
            }
        }
        return result;
    }

    public static void main(String[] args) {
        String test = "abcsbca";
        System.out.println(search(test, "bc"));
    }
}
