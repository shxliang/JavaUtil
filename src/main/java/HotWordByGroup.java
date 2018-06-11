import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import redis.clients.jedis.Tuple;
import scala.Tuple2;
import util.MapUtil;

import java.util.*;

/**
 * 分组统计topN词频
 *
 * @author lsx
 * @date 2016/10/24
 */

public class HotWordByGroup {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "D:\\winutils");
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/wx/wx_keyword.parquet")
                .filter("keywords IS NOT NULL");

        JavaRDD<Row> rowRDD = dataFrame.select("name", "keywords")
                .toJavaRDD()
                .flatMapToPair(new PairFlatMapFunction<Row, String, Integer>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Row row) throws Exception {
                        List<Tuple2<String, Integer>> result = new ArrayList<>();
                        String name = row.getString(0);
                        String[] words = row.getString(1).split(" ");
                        for (String word : words) {
                            result.add(new Tuple2<String, Integer>(name + ";" + word, 1));
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
                .mapToPair(new PairFunction<Tuple2<String, Integer>, String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        String[] parts = stringIntegerTuple2._1().split(";");
                        return new Tuple2<>(parts[0],
                                new Tuple2<String, Integer>(parts[1],
                                        stringIntegerTuple2._2()));
                    }
                })
                .groupByKey()
                .flatMapValues(new Function<Iterable<Tuple2<String, Integer>>, Iterable<Tuple2<String, Integer>>>() {
                    @Override
                    public Iterable<Tuple2<String, Integer>> call(Iterable<Tuple2<String, Integer>> tuple2s) throws Exception {
                        Map<String, Integer> map = new HashMap<>();
                        for (Tuple2<String, Integer> tup2 : tuple2s) {
                            map.put(tup2._1(), tup2._2());
                        }
                        map = MapUtil.sortByValue(map);
                        List<Tuple2<String, Integer>> result = new LinkedList<>();
                        int count = 0;
                        int maxCount = 100;
                        for (Map.Entry<String, Integer> entry : map.entrySet()) {
                            if (count >= maxCount) {
                                break;
                            }
                            result.add(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));
                            count++;
                        }
                        return result;
                    }
                })
                .map(new Function<Tuple2<String, Tuple2<String, Integer>>, Row>() {
                    @Override
                    public Row call(Tuple2<String, Tuple2<String, Integer>> stringTuple2Tuple2) throws Exception {
                        return RowFactory.create(stringTuple2Tuple2._1(),
                                stringTuple2Tuple2._2()._1(),
                                stringTuple2Tuple2._2()._2());
                    }
                });
        StructType schema = new StructType(new StructField[]{
                new StructField("name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("word", DataTypes.StringType, false, Metadata.empty()),
                new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
        });
        DataFrame resultDF = sqlContext.createDataFrame(rowRDD, schema);

//        resultDF.show();

        resultDF.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("header", "true")
                .save("wxdata/wx_keyword.csv");

    }
}