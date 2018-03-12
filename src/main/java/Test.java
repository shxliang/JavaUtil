import edu.stanford.nlp.ie.crf.CRFClassifier;
import edu.stanford.nlp.ling.CoreLabel;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import util.ParseUtil;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 *
 * @author lsx
 * @date 2017/1/7
 */
public class Test {
    public static final String mysqlUrl = "jdbc:mysql://90.90.90.101:3306/wxb_screen";
    public static final String user = "root";
    public static final String password = "1cc886c6c6b8";

    public static void main(String[] args) throws IOException, InterruptedException {
//        System.setProperty("hadoop.home.dir","D:\\winutils");
//
//        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("text");
//
//        JavaSparkContext jsc = new JavaSparkContext(sc);
//        SQLContext sqlContext = new SQLContext(jsc);
//
//        DataFrame test = sqlContext.read()
//                .parquet("hdfs://90.90.90.5:8020/ddp/today");
//        System.out.println(test.count());


        List<String> test = new ArrayList<>();
        test.add("a");
        test.add("b");
        String s = "a";
        System.out.println(test.contains(s));

    }


    private static void registerUDF(SQLContext sqlContext)
    {
        sqlContext.udf().register("getText", new UDF1<byte[], String>() {
            @Override
            public String call(byte[] s) throws Exception {
                if(s == null)
                {
                    return null;
                }
                String content = new String(s);
                return ParseUtil.formatHtml(content);
            }
        }, DataTypes.StringType);
    }

    public static class CustomThreadFactory implements ThreadFactory
    {
        private int counter;
        private String name;
        private List<String> stats;

        public CustomThreadFactory(String name)
        {
            counter = 1;
            this.name = name;
            stats = new ArrayList<String>();
        }

        @Override
        public Thread newThread(Runnable runnable)
        {
            Thread t = new Thread(runnable, name + "-Thread_" + counter);
            counter++;
            stats.add(String.format("Created thread %d with name %s on %s \n", t.getId(), t.getName(), new Date()));
            return t;
        }

        public String getStats()
        {
            StringBuffer buffer = new StringBuffer();
            Iterator<String> it = stats.iterator();
            while (it.hasNext())
            {
                buffer.append(it.next());
            }
            return buffer.toString();
        }
    }
}
