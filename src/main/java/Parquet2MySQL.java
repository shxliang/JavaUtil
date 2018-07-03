import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.Properties;

/**
 *
 * @author lsx
 * @date 2018/7/3
 */
public class Parquet2MySQL {
    /**
     * 数据库连接地址
     */
    private static String url = "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=utf8&autoReconnect=true";

    /**
     * 用户名
     */
    private static String user = "root";

    /**
     * 密码
     */
    private static String password = "root";

    /**
     * 表名
     */
    private static String table = "mrp_data";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        /*
        初始化Spark环境
         */
        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        // 从HDFS读取数据
        DataFrame inputDF = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/ddp/year=2018/month=201807/day=20180701");

        /*
        设置数据库用户名和密码
         */
        Properties properties = new Properties();
        properties.setProperty("user", user);
        properties.setProperty("password", password);
        properties.setProperty("driver", "com.mysql.jdbc.Driver");

        // 存入MySQL
        inputDF.select("docId", "title", "text")
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(url, table, properties);
    }
}
