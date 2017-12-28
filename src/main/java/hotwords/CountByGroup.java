package hotwords;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

/**
 *
 * @author lsx
 * @date 2017/12/27
 */
public class CountByGroup {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/lgxy/glg_docClass.parquet");

        sqlContext.udf().register("trimDocClass", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[0];
            }
        }, DataTypes.StringType);

        dataFrame = dataFrame.withColumn("newDocClass",
                functions.callUDF("trimDocClass",
                        functions.col("docClass")));

        dataFrame.select("docClass", "title", "removedText").show(200, false);

//        dataFrame.groupBy("newDocClass").count().show();
    }
}
