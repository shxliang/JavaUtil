import net.sourceforge.pinyin4j.PinyinHelper;

import java.util.Arrays;

/**
 * @author lsx
 * @date 2018/4/4
 */
public class PinYinDemo {
    public static void main(String[] args) {
        System.out.println(Arrays.toString(PinyinHelper.toHanyuPinyinStringArray('梁')));
    }
}
