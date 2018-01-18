package util;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import java.util.regex.Pattern;

/**
 *
 * @author lsx
 * @date 2017/7/7
 */
public class ParseUtil {
    private static Pattern style_reg = Pattern.compile("<style[^>]*>[\\s\\S]*?</style>");
    private static String comment_reg = "<!--.*?-->";
    private static Pattern tag_reg = Pattern.compile("</?[^>]+>");
    private static Pattern space_reg = Pattern.compile("(&nbsp;| |　)");
//    private static String inline_reg = "(?<=[^\\n。])\\n(?=[^\\n])";
    private static String inline_reg = "(?<=[^\\n。；：(第\\d+?号)])\\n(?=[^(\\n|书记员|(代理)?审判|人民陪审|(二w{3}年))])";
    private static String newline_reg = "(?<=[^(：|；|。|\\n|(\\d+?号)|月\\w{1,2}日)])\\n\\n" + "(?=[^(\\n|书记员|审判|人民陪审|" +
            "(二w{3}年))])";
    private static String br_reg = "\\n{3,}";

    public static String formatHtml(String htmlStr) {
        htmlStr = Jsoup.clean(htmlStr, Whitelist.relaxed());

        htmlStr = style_reg.matcher(htmlStr).replaceAll("");
        htmlStr = tag_reg.matcher(htmlStr).replaceAll("");
        htmlStr = space_reg.matcher(htmlStr).replaceAll("");
//        htmlStr = Pattern.compile(inline_reg).matcher(htmlStr).replaceAll("");
//        htmlStr = Pattern.compile(newline_reg).matcher(htmlStr).replaceAll("");
//        htmlStr = Pattern.compile(br_reg).matcher(htmlStr).replaceAll("\n\n");

        return htmlStr.replaceAll("[\n\r]","");
    }
}
