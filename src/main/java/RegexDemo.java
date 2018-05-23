import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * @author lsx
 * @date 2018/5/23
 */
public class RegexDemo {
    private static Pattern pattern = Pattern.compile("<P>(.+?)</P>");

    public static void main(String[] args) {
        String text = "啊啊啊<P>人人人</P>啊啊啊啊<P>人人</P>啊啊啊";
        Matcher matcher = pattern.matcher(text);
        while (matcher.find()){
            System.out.println(matcher.group(1));
            System.out.println(matcher.start());
            System.out.println(matcher.end());
        }
    }
}
