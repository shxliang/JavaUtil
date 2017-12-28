import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author lsx
 * @date 2016/10/24
 */

public class FinalTest {
    public static void main(String[] args)
    {
//        System.setProperty("hadoop.home.dir", "D:\\winutils");
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/lgxy/glg_parsed.parquet");

        JavaRDD<Row> rowRDD = dataFrame.select("keywords")
                .toJavaRDD()
                .flatMapToPair(new PairFlatMapFunction<Row, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Row row) throws Exception {
                        List<Tuple2<String, Integer>> result = new ArrayList<>();
                        String[] words = row.getString(0).split(" ");
                        for (String word : words)
                        {
                            result.add(new Tuple2<String, Integer>(word, 1));
                        }
                        return result;
                    }
                })
                .reduceByKey(new Function2<Integer, Integer, Integer>() {
                    @Override
                    public Integer call(Integer integer, Integer integer2) throws Exception {
                        return integer + integer2;
                    }
                })
                .map(new Function<Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return RowFactory.create(stringIntegerTuple2._1(), stringIntegerTuple2._2());
                    }
                });
        StructType schema = new StructType(new StructField[]{
                new StructField("word", DataTypes.StringType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
        });
        DataFrame resultDF = sqlContext.createDataFrame(rowRDD, schema);
        resultDF = resultDF.sort(functions.col("count").desc()).limit(150);
//        resultDF.show();
        resultDF.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/lgxy/glg_hotwords.parquet");

    }
}