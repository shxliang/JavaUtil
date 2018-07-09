import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

/**
 * @author lsx
 * @date 2017/9/30
 */
public class TTest {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        jsc.setLogLevel("WARN");
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/topic/segmented.parquet");

//        dataFrame.show();
//        dataFrame.printSchema();
        System.out.println(dataFrame.count());
//        dataFrame.groupBy("class").count().show();

//        dataFrame.repartition(1)
//                .write()
//                .mode(SaveMode.Overwrite)
//                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/topic/keyphrase_filter.parquet");

        jsc.stop();
    }
}
