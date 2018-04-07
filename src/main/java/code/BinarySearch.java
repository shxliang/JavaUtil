package code;

/**
 *
 * @author lsx
 * @date 2018/4/7
 */
public class BinarySearch {
    public static int binarySearch(int[] p, int from, int to, int value)
    {
        int start = from - from;
        int end = to - from;

        if(start > end || p.length < 1)
        {
            return -1;
        }
        if(start == end)
        {
            if(p[start + from] == value)
            {
                return(start + from);
            }
            return -1;
        }
        else
        {
            int temp = p[end/2];
            if(value == temp)
            {
                return end/2 + from;
            }
            else if(value < temp)
            {
                return binarySearch(p, start + from, end/2 - 1 + from, value);
            }
            else
            {
                return binarySearch(p, end/2 + 1 + from, end + from, value);
            }
        }
    }

    public static int binarySearch(int[] p, int value)
    {
        if (p.length < 1)
        {
            return -1;
        }
        int from = 0;
        int to = p.length - 1;
        return binarySearch(p, from, to, value);
    }

    public static void main(String[] args) {
        int[] p = new int[]{1, 2, 2, 3};
        System.out.println(binarySearch(p, 3));
    }
}
