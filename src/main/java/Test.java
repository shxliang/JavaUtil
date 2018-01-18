import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.*;
import util.ParseUtil;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author lsx
 * @date 2017/1/7
 */
public class Test {
    public static final String mysqlUrl = "jdbc:mysql://90.90.90.101:3306/wxb_screen";
    public static final String user = "root";
    public static final String password = "1cc886c6c6b8";

    public static void main(String[] args) throws IOException {
//        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

//        registerUDF(sqlContext);
//
//        String querySql = "(SELECT * FROM page WHERE analyzed=0 AND text IS NOT NULL) temp";
//        DataFrame inputData = sqlContext.read()
//                .format("jdbc")
//                .option("url",mysqlUrl)
//                .option("user",user)
//                .option("password",password)
//                .option("dbtable",querySql)
//                .option("driver", "com.mysql.jdbc.Driver")
//                .load();
//        inputData = inputData.withColumn("newText",
//                functions.callUDF("getText",
//                        functions.col("text")));
//        inputData.select("newText").show(false);

        DataFrame dataFrame = sqlContext
                .read()
                .parquet("hdfs://90.90.90.5:8020/ddp/today");
        System.out.println(dataFrame.count());

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
}
