import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.common.Term;
import org.fnlp.nlp.cn.CNFactory;
import org.fnlp.util.exception.LoadModelException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author lsx
 * @date 2018/4/2
 */
public class FuDanNLPDemo {
    public static void main(String[] args) throws LoadModelException {
        String text = "“增”与“减”、“高”与“低”、“升”与“降”，是一组矛盾对立的概念。但是，在政府工作报告中，这些矛盾的双方，却显得如此和谐统一。一个个涉及民生领域的数字、指标，无论增也好、减也好，高也好、低也好，升也好、降也好，全都指向人民对美好生活的新期待。这是一支充满“民生温度”的变奏曲，这是一部以“人民为中心”的辩证法。\\n  过去5年，在以习近平同志为核心的党中央亲切关怀下，一项项民生福祉惠及13亿多中国人。从增的方面看：城镇新增就业6600万人以上，13亿多人口的大国实现了比较充分就业；居民收入年均增长7.4%，超过经济增速，形成世界上人口最多的中等收入群体；出境旅游人次由8300万增加到1.3亿多；城镇化率从52.6%提高到58.5%……从减的方面看：脱贫攻坚取得决定性进展，贫困人口减少6800多万，贫困发生率由10.2%下降到3.1%；主要污染物排放量持续下降，重点城市重污染天数减少一半，沙化土地面积年均缩减近2000平方公里，绿色发展呈现可喜局面……这是一份沉甸甸的成绩单，此消彼长的数字，向上向下的箭头，深刻见证着以“人民为中心”的发展思想。\\n  聆听这支“增”与“减”的变奏曲，我们感受到了什么是不忘初心，牢记使命。习进平总书记在党的十九大报告中指出：中国共产党人的初心和使命，就是为中国人民谋幸福，为中华民族谋复兴。这个初心和使命是激励中国共产党人不断前进的根本动力。为此，党和政府始终把解决民生问题作为最大的政治，把改善民生作为最大的政绩，始终把人民对美好生活的向往作为奋斗目标。\\n  聆听这支“增”与“减”的变奏曲，我们感受到了什么是为梦想不懈奋斗。习近平总书记要求全党同志“一定要永远与人民同呼吸、共命运、心连心，永远把人民对美好生活的向往作为奋斗目标，以永不懈怠的精神状态和一往无前的奋斗姿态，继续朝着实现中华民族伟大复兴的宏伟目标奋勇前进”。新时代是奋斗者的时代。政府工作报告中每一个数字的增加或者减少，都彰显了党和政府带领全体人民不懈奋斗的坚定决心。\\n  聆听这支“增”与“减”的变奏曲，我们感受到的是各族群众与日俱增的获得感幸福感安全感。在以习近平同志为核心的党中央坚强领导下，各级党委政府抓住人民最关心最直接最现实的利益问题，既尽力而为，又量力而行，一件事情接着一件事情办，一年接着一年干，民生建设的丰硕成果不断提升着各族群众的获得感幸福感，激励我们为创造更加美好的明天而砥砺前行、团结奋斗！";

        // 创建中文处理工厂对象，并使用“models”目录下的模型文件初始化
        CNFactory factory = CNFactory.getInstance("models");

        String posResult = factory.tag2String(text);
        // 显示标注结果
        System.out.println(posResult);

        // 使用标注器对包含实体名的句子进行标注，得到结果
        HashMap<String, String> nerResult = factory.ner(text);
        // 显示标注结果
        System.out.println(nerResult);

        List<Term> hanlpResult = NShortSegment.parse(text);
        System.out.println(hanlpResult);
    }
}
