import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;

/**
 * Created by lsx on 2016/10/24.
 */

public class FinalTest {
    public static void main(String[] args)
    {
//        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame test = sqlContext.read().parquet("hdfs://90.90.90.5:8020/user/lsx/MRPResult/p_allData.parquet/year=2017/month=08/day=09");
        test.registerTempTable("test");

//        test.show();
        test.filter("docId='WSVg3l1hsQHsE1MflWSCH4lkOTZR9XP2|1502258014086|d2498'").show();
//        System.out.println(test.count());
//        test.printSchema();
//        System.out.println(test.select("tags").toJSON().first());


//        test.repartition(1)
//                .write()
//                .mode(SaveMode.Overwrite)
//                .parquet("hdfs://90.90.90.5:8020/user/lsx/debug/data.parquet");

    }
}