
import java.io.*;
import java.util.List;
import java.util.Properties;

import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;


/**
 * This is a very simple demo of calling the Chinese Word Segmenter
 * programmatically.  It assumes an input file in UTF8.
 * <p/>
 * <code>
 * Usage: java -mx1g -cp seg.jar StanfordSegmentDemo fileName
 * </code>
 * This will run correctly in the distribution home directory.  To
 * run in general, the properties for where to find dictionaries or
 * normalizations have to be set.
 *
 * @author Christopher Manning
 */

public class StanfordSegmentDemo {

    private static final String basedir = "C:\\Users\\lsx\\IdeaProjects\\MyAwesomeSpark\\src\\main\\java\\data";

    public static void main(String[] args) throws Exception {
        System.setOut(new PrintStream(System.out, true, "utf-8"));

        Properties props = new Properties();
        props.setProperty("sighanCorporaDict", basedir);
        // props.setProperty("NormalizationTable", "data/norm.simp.utf8");
        props.setProperty("normTableEncoding", "UTF-8");
        // below is needed because CTBSegDocumentIteratorFactory accesses it
        props.setProperty("serDictionary", basedir + "/dict-chris6.ser.gz");
        if (args.length > 0) {
            props.setProperty("testFile", args[0]);
        }
        props.setProperty("inputEncoding", "UTF-8");
        props.setProperty("sighanPostProcessing", "true");

        CRFClassifier<CoreLabel> segmenter = new CRFClassifier<>(props);
        segmenter.loadClassifierNoExceptions(basedir + "/ctb.gz", props);
//    for (String filename : args) {
//      segmenter.classifyAndWriteAnswers(filename);
//    }

        String sample = "我骑共享单车去了。";
        List<String> segmented = segmenter.segmentString(sample);
        System.out.println(segmented);
    }
}
