package util;

/**
 * @author lsx
 * @date 2018/4/25
 */
public class LCS {
    public static double getLCSSim(String parentStr, String childStr) {
        int lcsLength = LCSLength(parentStr, childStr);
        return (double) lcsLength / childStr.length();
    }

    private static int LCSLength(String str1, String str2) {
        char[] chars1 = str1.toCharArray();
        char[] chars2 = str2.toCharArray();

        int[] c = new int[chars2.length + 1];

        for (int j = 0; j < c.length; j++) {
            c[j] = 0;
        }
        for (int i = 0; i < chars1.length; i++) {
            for (int j = 1; j < c.length; j++) {
                if (chars1[i] == chars2[j - 1]) {
                    c[j] = c[j - 1] + 1;
                } else if (c[j - 1] > c[j]) {
                    c[j] = c[j - 1];
                }
            }
        }
        return c[c.length - 1];
    }
}
