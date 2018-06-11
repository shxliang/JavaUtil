import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.spark.sql.functions.count;

/**
 * 解析搜狗实验室数据
 *
 * @author lsx
 * @date 2017/5/22
 */
public class SogouDataParser {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        List<Row> classLabel = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load("file:///E:\\数据集\\搜狗实验室\\sohu_categories_2012.csv")
                .collectAsList();
        final Map<String, String> classLabelMap = new HashMap<>();
        for (Row r : classLabel) {
            classLabelMap.put(r.getString(1), r.getString(0));
        }

        DataFrame data = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .load("file:///E:\\数据集\\搜狗实验室\\sogoucs_reduced.csv");
        data.registerTempTable("data");

        sqlContext.udf().register("getClass", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                Set<String> set = classLabelMap.keySet();
                for (String str : set) {
                    if (s.startsWith(str)) {
                        return classLabelMap.get(str);
                    }
                }
                return null;
            }
        }, DataTypes.StringType);


        DataFrame result = sqlContext.sql("SELECT docno," +
                "getClass(url) AS class," +
                "contenttitle," +
                "content " +
                "FROM data");

        result = result.filter(result.col("class").isNotNull())
                .filter(result.col("contenttitle").isNotNull())
                .filter(result.col("content").isNotNull());

        result.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://90.90.90.5:8020/USER/lsx/test/Sohu/sogoucs_reduced.parquet");

        System.out.println(result.count());
    }
}
