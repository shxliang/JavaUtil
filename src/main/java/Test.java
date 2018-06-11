import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import util.HtmlUtil;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadFactory;

/**
 * @author lsx
 * @date 2017/1/7
 */
public class Test {
    public static final String mysqlUrl = "jdbc:mysql://90.90.90.101:3306/wxb_screen";
    public static final String user = "root";
    public static final String password = "1cc886c6c6b8";

    public static void main(String[] args) throws IOException, InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame df = sqlContext.read()
                .json("C:\\Users\\lsx\\Downloads\\民事一审案件_9015-100_2.txt");
        List<Tuple2<String, String>> tuple2s = df.select("case_code", "judgment")
                .toJavaRDD()
                .map(new Function<Row, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2<>(row.getString(0),
                                row.getString(1));
                    }
                })
                .collect();
        String dirPath = "民事一审案件_9015-100_2/";
        for (Tuple2<String, String> tuple2 : tuple2s) {
            String caseId = tuple2._1();
            FileWriter writer = new FileWriter(dirPath + caseId);
            BufferedWriter bw = new BufferedWriter(writer);
            String[] parts = tuple2._2().split("[\n\r]");
            for (String part : parts) {
                if (part.trim().length() < 1) {
                    continue;
                }
                bw.write(part + "\n");
            }
            bw.close();
            writer.close();
        }
    }


    private static void registerUDF(SQLContext sqlContext) {
        sqlContext.udf().register("getText", new UDF1<byte[], String>() {
            @Override
            public String call(byte[] s) throws Exception {
                if (s == null) {
                    return null;
                }
                String content = new String(s);
                return HtmlUtil.formatHtml(content);
            }
        }, DataTypes.StringType);

        sqlContext.udf().register("containPersonName", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                boolean isContain = s.contains("习近平")
                        || s.contains("李源潮")
                        || s.contains("李克强")
                        || s.contains("张高丽")
                        || s.contains("刘延东")
                        || s.contains("汪洋")
                        || s.contains("马凯")
                        || s.contains("杨晶")
                        || s.contains("常万全")
                        || s.contains("杨洁篪")
                        || s.contains("郭声琨")
                        || s.contains("王勇");

                return isContain ? "true" : "false";
            }
        }, DataTypes.StringType);
    }

    public static class CustomThreadFactory implements ThreadFactory {
        private int counter;
        private String name;
        private List<String> stats;

        public CustomThreadFactory(String name) {
            counter = 1;
            this.name = name;
            stats = new ArrayList<String>();
        }

        @Override
        public Thread newThread(Runnable runnable) {
            Thread t = new Thread(runnable, name + "-Thread_" + counter);
            counter++;
            stats.add(String.format("Created thread %d with name %s on %s \n", t.getId(), t.getName(), new Date()));
            return t;
        }

        public String getStats() {
            StringBuffer buffer = new StringBuffer();
            Iterator<String> it = stats.iterator();
            while (it.hasNext()) {
                buffer.append(it.next());
            }
            return buffer.toString();
        }
    }
}
