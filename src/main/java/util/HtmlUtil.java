package util;

import org.jsoup.Jsoup;
import org.jsoup.safety.Whitelist;

import java.util.regex.Pattern;

/**
 *
 * @author lsx
 * @date 2017/7/7
 */
public class HtmlUtil {
    private static Pattern style_reg = Pattern.compile("<style[^>]*>[\\s\\S]*?</style>");
    private static Pattern tag_reg = Pattern.compile("</?[^>]+>");
    private static Pattern space_reg = Pattern.compile("(&nbsp;| |　)");


    public static String formatHtml(String htmlStr) {
        htmlStr = Jsoup.clean(htmlStr, Whitelist.relaxed());

        htmlStr = style_reg.matcher(htmlStr).replaceAll("");
        htmlStr = tag_reg.matcher(htmlStr).replaceAll("");
        htmlStr = space_reg.matcher(htmlStr).replaceAll("");

        return htmlStr.replaceAll("[\n\r]","");
    }

    public static void main(String[] args) {
        String text = "<!DOCTYPE html PUBLIC \\\"-//W3C//DTD XHTML 1.0 Transitional//EN\\\" \\\"http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd\\\">\\n<html xmlns=\\\"http://www.w3.org/1999/xhtml\\\">\\n <head> \\n  <meta http-equiv=\\\"Content-Type\\\" content=\\\"text/html; charset=utf-8\\\"> \\n  <title>网站访问报错</title> \\n  <style type=\\\"text/css\\\">\\r\\n* { padding:0; margin:0;}\\r\\nli { list-style:none;}\\r\\nimg { border:none;}\\r\\n.clear { zoom:1;}\\r\\n.clear:after { content:'\\\\20'; clear:both; display:block;}\\r\\n\\r\\n.error-page { width:940px; margin:0 auto; padding-top:110px;}\\r\\n.error-page-left { width:440px; float:left; background:url(images/404-pic.gif) no-repeat 22px 0; height:478px;}\\r\\n.error-page-right { width:500px; float:left;}\\r\\n.error-page-right h3 { line-height:114px; font-size:22px; color:#333; font-weight:600; padding-top:10px;}\\r\\n.error-page-title { line-height:24px; font-size:14px; color:#333;}\\r\\n.error-page-title a { color:#0066cc; text-decoration:underline;}\\r\\n.error-page-txt { line-height:24px; padding-left:40px; font-size:14px; color:#333; padding-bottom:9px;}\\r\\n.error-page-txt a { color:#0066cc; text-decoration:underline;}\\r\\n</style> \\n </head> \\n <body> \\n  <div class=\\\"error-page\\\"> \\n   <div class=\\\"error-page-left\\\">\\n    &nbsp;\\n   </div> \\n   <div class=\\\"error-page-right\\\"> \\n    <h3>抱歉！该网站可能由于以下原因无法访问！</h3> \\n    <p class=\\\"error-page-title\\\">&gt;&gt;1、您访问的域名未绑定至主机；</p> \\n    <p class=\\\"error-page-txt\\\">解决方法：需要网站管理员登录<a href=\\\"http://cp.aliyun.com/\\\" target=\\\"_blank\\\">万网主机控制面板</a>绑定域名，阿里云账号请登录<a href=\\\"https://netcn.console.aliyun.com/core/host/list2\\\" target=\\\"_blank\\\">阿里云虚拟主机控制台</a>绑定。</p> \\n    <p class=\\\"error-page-title\\\">&gt;&gt;2、您正在使用IP访问；</p> \\n    <p class=\\\"error-page-txt\\\">解决方法：请尝试使用域名进行访问。</p> \\n    <p class=\\\"error-page-title\\\">&gt;&gt;3、该站点已被网站管理员停止；</p> \\n    <p class=\\\"error-page-txt\\\">解决方法：需要网站管理员登录<a href=\\\"http://cp.aliyun.com/\\\" target=\\\"_blank\\\">万网主机控制面板</a>开启站点，阿里云账号请登录<a href=\\\"https://netcn.console.aliyun.com/core/host/list2\\\" target=\\\"_blank\\\">阿里云虚拟主机控制台</a>进行开通。</p> \\n   </div> \\n  </div>   \\n </body>\\n</html>";
        System.out.println(formatHtml(text));
    }
}
