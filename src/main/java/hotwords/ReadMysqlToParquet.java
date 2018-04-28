package hotwords;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import util.HtmlUtil;

/**
 *
 * @author lsx
 * @date 2017/12/27
 */
public class ReadMysqlToParquet {
    public static final String MYSQL_URL = "jdbc:mysql://90.90.90.101:3306/crawlerdata";
    public static final String USER = "root";
    public static final String PASSWORD = "1cc886c6c6b8";

    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        registerUDF(sqlContext);

        String querySql = "(SELECT docid,title,text,nickname_id,posttime FROM routine_wxarticle WHERE hasText=1) temp";
        DataFrame wxData = sqlContext.read()
                .format("jdbc")
                .option("url", MYSQL_URL)
                .option("user", USER)
                .option("PASSWORD", PASSWORD)
                .option("dbtable",querySql)
                .option("driver", "com.mysql.jdbc.Driver")
                .load();
        wxData.registerTempTable("wxData");

        DataFrame wxGroup = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/17hotwords/wx_group.txt");
        wxGroup.registerTempTable("wxGroup");

        String authorQuerySql = "(SELECT groupid,nicknameId FROM routine_wxdata) temp";
        DataFrame wxAuthor = sqlContext.read()
                .format("jdbc")
                .option("url", MYSQL_URL)
                .option("user", USER)
                .option("PASSWORD", PASSWORD)
                .option("dbtable",authorQuerySql)
                .option("driver", "com.mysql.jdbc.Driver")
                .load();
        wxAuthor.registerTempTable("wxAuthor");

        wxAuthor = sqlContext.sql("SELECT wxGroup.groupName," +
                "wxAuthor.nicknameId " +
                "FROM wxGroup INNER JOIN wxAuthor " +
                "ON wxGroup.groupId=wxAuthor.groupid");
        wxAuthor.registerTempTable("wxAuthor");

        wxData = sqlContext.sql("SELECT wxData.docid," +
                "wxData.posttime," +
                "wxData.title," +
                "wxData.text," +
                "wxAuthor.groupName " +
                "FROM wxData INNER JOIN wxAuthor " +
                "ON wxData.nickname_id=wxAuthor.nicknameId");

        wxData = wxData.withColumn("newText",
                functions.callUDF("getText",
                        functions.col("text")));
        wxData = wxData.drop("text").withColumnRenamed("newText", "text");

        wxData.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/17hotwords/wx_data.parquet");

        jsc.stop();
    }

    private static void registerUDF(SQLContext sqlContext)
    {
        sqlContext.udf().register("getText", new UDF1<byte[], String>() {
            @Override
            public String call(byte[] s) throws Exception {
                String content = new String(s);
                return HtmlUtil.formatHtml(content);
            }
        }, DataTypes.StringType);
    }
}
