import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.ml.classification.*;
import org.apache.spark.ml.clustering.*;
import org.apache.spark.ml.feature.*;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.*;
import scala.Tuple2;
import scala.util.*;

import java.io.IOException;
import java.util.Set;

/**
 * Created by lsx on 2017/1/7.
 */
public class Test {
    public static void main(String[] args) throws IOException {
//        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/lsx/17hotwords/mrp_data.parquet");
        dataFrame.select("docClass").distinct().show(100);

        jsc.stop();
    }
}
