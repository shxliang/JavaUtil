import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.*;
import scala.util.*;

import java.io.IOException;

/**
 * Created by lsx on 2017/1/7.
 */
public class Test {
    public static void main(String[] args) throws IOException {
//        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        sqlContext.udf().register("combine", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "。" + s2;
            }
        }, DataTypes.StringType);


        DataFrame allDF = sqlContext.read()
                .parquet("/Users/lsx/Desktop/TextClassification/removed.parquet");
        DataFrame[] dataFrames = allDF.randomSplit(new double[]{0.8,0.2},1231);
        DataFrame trainDF = dataFrames[0];
        DataFrame testDF = dataFrames[1];

        trainDF.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/Users/lsx/Desktop/TextClassification/train.parquet");
        testDF.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("/Users/lsx/Desktop/TextClassification/test.parquet");

        jsc.stop();
    }
}
