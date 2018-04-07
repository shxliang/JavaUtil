package code;

import java.util.Arrays;

/**
 * @author lsx
 * @date 2018/4/6
 */
public class QuickSort {
    public static void quickSort(int[] p, int start, int end) {
        if (end == start) {
            return;
        } else {
            int key = p[start];
            int i = start;
            int j = end;
            int temp;

            while (j > i) {

                if (p[j] < key) {
                    while (i < j) {
                        if (p[i] > key) {
                            temp = p[i];
                            p[i] = p[j];
                            p[j] = temp;
                            break;
                        }
                        i++;
                    }
                }
                if (i == j) {
                    break;
                }
                j--;
            }

            p[start] = p[i];
            p[i] = key;
            quickSort(p, start, (i - 1) > start ? (i - 1) : start);
            quickSort(p, i + 1 < end ? (i + 1) : end, end);
        }
    }

    public static void main(String[] args) {
        int[] p = new int[]{6, 1, 2, 7, 9, 3, 4, 5, 10, 8};
        quickSort(p, 0, p.length - 1);
        System.out.println(Arrays.toString(p));
    }
}
