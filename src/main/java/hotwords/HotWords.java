package hotwords;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import utils.MapUtil;

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

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/lsx/17hotwords/mrp_data.parquet");

        dataFrame = dataFrame.filter("docClass IN " +
                "('社会', '军事', '体育', '时事', '娱乐', '旅游', '汽车')");

        String[] colNames = new String[]{"keywords", "textPerson", "textPlace", "segmentedAll"};
        String pathPrefix = "hdfs://90.90.90.5:8020/user/lsx/17hotwords/";

        for (final String colName : colNames) {
            JavaRDD<Row> rowRDD = dataFrame.select("docClass", colName)
                    .toJavaRDD()
                    .flatMapToPair(new PairFlatMapFunction<Row, String, String>() {
                        @Override
                        public Iterable<Tuple2<String, String>> call(Row row) throws Exception {
                            List<Tuple2<String, String>> result = new ArrayList<>();
                            String words = row.getString(1);
                            if (words == null || words.length() < 1) {
                                return result;
                            }
                            if ("segmentedAll".equals(colName)) {
                                String text = words.replaceAll(" ", "");
                                String newWords = WordFind.getWords(text, words, 0.5);
                                result.add(new Tuple2<>(row.getString(0), newWords));
                            } else {
                                result.add(new Tuple2<>(row.getString(0), words));
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
                            for (String word : wordsArray) {
                                if ("textPerson".equals(colName))
                                {
                                    if ((word.length() == 2 && word.endsWith("某")) ||
                                            (word.length() ==3 && word.endsWith("某某")))
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
                            int count = 0;
                            for (Map.Entry<String, Integer> entry : sortedMap.entrySet()) {
                                if (count >= 200) {
                                    break;
                                }
                                result.add(RowFactory.create(key, entry.getKey(), entry.getValue()));
                                count++;
                            }
                            return result;
                        }
                    });
            StructType schema = new StructType(new StructField[]{
                    new StructField("docClass", DataTypes.StringType, false, Metadata.empty()),
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
}
