package hotwords;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import util.MapUtil;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author lsx
 * @date 2017/12/26
 */
public class HotWords {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        registerUDF(sqlContext);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/17hotwords/wx_data_parsed.parquet");

        dataFrame = dataFrame.withColumn("allPerson",
                functions.callUDF("combine",
                        functions.col("titlePerson"),
                        functions.col("textPerson")));
        dataFrame = dataFrame.withColumn("allPlace",
                functions.callUDF("combine",
                        functions.col("titlePlace"),
                        functions.col("textPlace")));
        dataFrame = dataFrame.withColumn("personAndPlace",
                functions.callUDF("combine",
                        functions.col("allPerson"),
                        functions.col("allPlace")));
        dataFrame = dataFrame.withColumn("segmentedAll",
                functions.callUDF("combine",
                        functions.col("segmentedTitle"),
                        functions.col("segmentedText")));
        dataFrame = dataFrame.withColumn("month",
                functions.callUDF("getMonth",
                        functions.col("posttime")));
        dataFrame = dataFrame.cache();

        String[] colNames = new String[]{"keywords", "personAndPlace", "segmentedAll"};
        String[] monthes = new String[]{"2017-01", "2017-02", "2017-03", "2017-04", "2017-05", "2017-06",
                                        "2017-07", "2017-08", "2017-09", "2017-10", "2017-11", "2017-12"};
        String pathPrefix = "hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/17hotwords/wx_";

        for (final String colName : colNames) {
            JavaRDD<Row> rowRDD = dataFrame.select("groupName", colName, "month")
                    .toJavaRDD()
                    .flatMapToPair(new PairFlatMapFunction<Row, String, String>() {
                        @Override
                        public Iterable<Tuple2<String, String>> call(Row row) throws Exception {
                            List<Tuple2<String, String>> result = new ArrayList<>();
                            String words = row.getString(1);
                            if (words == null || words.length() < 1) {
                                return result;
                            }
                            String key = row.getString(0) + ";" + row.getString(2);
                            if ("segmentedAll".equals(colName)) {
                                String text = words.replaceAll(" ", "");
                                String newWords = WordFind.getWords(text, words, 0.5);
                                result.add(new Tuple2<>(key, newWords));
                            } else {
                                result.add(new Tuple2<>(key, words));
                            }

                            return result;
                        }
                    }).mapToPair(new PairFunction<Tuple2<String,String>, String, Map<String, Integer>>() {
                        @Override
                        public Tuple2<String, Map<String, Integer>> call(Tuple2<String, String> stringTuple4Tuple2) throws Exception {
                            String key = stringTuple4Tuple2._1();
                            String words = stringTuple4Tuple2._2();
                            Map<String, Integer> wordCount = new HashMap<>();
                            String[] wordsArray = words.split(" ");
                            for (String word : wordsArray)
                            {
                                if ("personAndPlace".equals(colName))
                                {
                                    if ((word.length() == 2 && word.endsWith("某")) ||
                                            (word.length() ==3 &&
                                                    (word.endsWith("某某") ||
                                                            word.endsWith("先生") ||
                                                            word.endsWith("女士"))))
                                    {
                                        continue;
                                    }
                                }
                                if (word.length() == 1)
                                {
                                    continue;
                                }
                                if (wordCount.containsKey(word)) {
                                    wordCount.put(word, wordCount.get(word) + 1);
                                } else {
                                    wordCount.put(word, 1);
                                }
                            }
                            return new Tuple2<>(key, wordCount);
                        }
                    }).reduceByKey(new Function2<Map<String, Integer>, Map<String, Integer>, Map<String, Integer>>() {
                        @Override
                        public Map<String, Integer> call(Map<String, Integer> stringIntegerMap, Map<String, Integer> stringIntegerMap2) throws Exception {
                            for (Map.Entry<String, Integer> entry : stringIntegerMap2.entrySet()) {
                                String key = entry.getKey();
                                int value = entry.getValue();
                                if (stringIntegerMap.containsKey(key)) {
                                    stringIntegerMap.put(key, stringIntegerMap.get(key) + value);
                                } else {
                                    stringIntegerMap.put(key, value);
                                }
                            }
                            return stringIntegerMap;
                        }
                    }).flatMap(new FlatMapFunction<Tuple2<String, Map<String, Integer>>, Row>() {
                        @Override
                        public Iterable<Row> call(Tuple2<String, Map<String, Integer>> stringMapTuple2) throws Exception {
                            Map<String, Integer> sortedMap = MapUtil.sortByValue(stringMapTuple2._2());
                            List<Row> result = new ArrayList<>();
                            String key = stringMapTuple2._1();
                            String[] parts = key.split(";");
                            String groupName = parts[0];
                            String month = parts[1];
                            int count = 0;
                            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                                if (count >= 200) {
                                    break;
                                }
                                result.add(RowFactory.create(groupName, month, entry.getKey(), entry.getValue()));
                                count++;
                            }
                            return result;
                        }
                    });
            StructType schema = new StructType(new StructField[]{
                    new StructField("groupName", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("month", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("word", DataTypes.StringType, false, Metadata.empty()),
                    new StructField("count", DataTypes.IntegerType, false, Metadata.empty())
            });
            DataFrame resultDF = sqlContext.createDataFrame(rowRDD, schema);
            resultDF.repartition(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .format("com.databricks.spark.csv")
                    .option("header", "true")
                    .save(pathPrefix + colName + "_NWF.csv");

//            resultDF.show();
        }

        jsc.stop();
    }

    private static void registerUDF(SQLContext sqlContext)
    {
        sqlContext.udf().register("combine", new UDF2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                if (s == null || s.length() < 1)
                {
                    return s2;
                }
                return s + " " + s2;
            }
        }, DataTypes.StringType);

        sqlContext.udf().register("getMonth", new UDF1<Timestamp, String>() {
            @Override
            public String call(Timestamp t) throws Exception {
                if (t == null)
                {
                    return "";
                }
                return t.toString().substring(0, 7);
            }
        }, DataTypes.StringType);
    }
}
