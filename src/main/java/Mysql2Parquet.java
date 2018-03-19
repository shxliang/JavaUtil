import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import util.ParseUtil;

/**
 *
 * @author lsx
 * @date 2018/3/16
 */
public class Mysql2Parquet {
    private static String url = "jdbc:mysql://90.90.90.101:3306/wxb_screen";
    private static String user = "root";
    private static String password = "1cc886c6c6b8";

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("test");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

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

        String querySql = "(SELECT * " +
                "FROM crawlerdata.routine_wxarticle " +
                "WHERE (name='微七星关' " +
                "OR name='毕节发布' " +
                "OR name='赫章发布' " +
                "OR name='威宁发布' " +
                "OR name='珙桐纳雍' " +
                "OR name='织金发布' " +
                "OR name='黔西宣传' " +
                "OR name='金沙发布' " +
                "OR name='大方发布') " +
                "AND posttime>='2018-01-01 00:00:00' " +
                "AND posttime<='2018-03-14 00:00:00') temp";

        DataFrame inputData = sqlContext.read()
                .format("jdbc")
                .option("url", url)
                .option("user", user)
                .option("password", password)
                .option("dbtable",querySql)
                .option("driver", "com.mysql.jdbc.Driver")
                .load();

        inputData = inputData.withColumn("content",
                functions.callUDF("getText", functions.col("text")));

        inputData.select("docid", "name", "title", "content")
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("wxdata/wx.parquet");

    }
}
