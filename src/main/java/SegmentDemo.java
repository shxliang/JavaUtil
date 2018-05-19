import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.seg.CRF.CRFSegment;
import com.hankcs.hanlp.seg.Dijkstra.DijkstraSegment;
import com.hankcs.hanlp.seg.NShort.NShortSegment;
import com.hankcs.hanlp.seg.Segment;
import org.ansj.domain.Result;
import org.ansj.domain.Term;
import org.ansj.library.CrfLibrary;
import org.ansj.recognition.Recognition;
import org.ansj.recognition.arrimpl.AsianPersonRecognition;
import org.ansj.recognition.impl.NatureRecognition;
import org.ansj.splitWord.analysis.ToAnalysis;

import java.util.List;

/**
 *
 * @author lsx
 * @date 2018/3/20
 */
public class SegmentDemo {
    public static void main(String[] args) {
        String text = "4月的广西昭平，高高低低的小土丘上，一排排茶树沿山势整齐地梯级而上，茵绿油亮，清香暗浮。40多岁的马圣村农民黄金凤在当地的有机茶园打工，一年能挣到两万多元，“我家里一亩地也种上了茶。现在白天来茶园上班，下班后照看自家茶树，还能照顾老人孩子。";
        List<String> words = CrfLibrary.get().cut(text);
        List<Term> recognition = new NatureRecognition().recognition(words, 0) ;
        System.out.println(recognition);

        for (Term term : recognition) {
            Result terms = ToAnalysis.parse(term.getName());
            System.out.println(terms);
        }
    }
}
