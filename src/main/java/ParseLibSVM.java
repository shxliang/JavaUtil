import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by lsx on 2016/9/27.
 */
public class ParseLibSVM {


    public static class WordList implements Serializable
    {
        public String wordId;
        public String word;

        public String getWordId() {
            return wordId;
        }

        public void setWordId(String wordId) {
            this.wordId = wordId;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }
    }

    public static class Parsed implements Serializable
    {
        public String id;
        public String kv;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getKv() {
            return kv;
        }

        public void setKv(String kv) {
            this.kv = kv;
        }
    }


    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("ParseLibSVM");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame removed = sqlContext.read().parquet("hdfs://108.108.108.15/user/root/nlp/testText.parquet");
        removed.registerTempTable("removed");


        //第一列为id(string)，第二列为word(string)，第三列为count(long)
        DataFrame counted = sqlContext.read().parquet("hdfs://108.108.108.15/user/root/nlp/counted.parquet").cache();

        counted.registerTempTable("counted");




        List<Row> rowList = sqlContext.sql("SELECT DISTINCT word FROM counted").toJavaRDD().collect();


        ArrayList<String> wordListLocal = new ArrayList<>();
        ArrayList<String> wordListMerge = new ArrayList<>();


        for (int i=0;i<rowList.size();i++)
        {
            wordListLocal.add(rowList.get(i).getString(0).trim());
            wordListMerge.add(rowList.get(i).getString(0).trim()+","+i);
        }

        final Broadcast<ArrayList<String>> broadcastWordList = jsc.broadcast(wordListLocal);

        DataFrame wordList = sqlContext.createDataFrame(jsc.parallelize(wordListMerge).map(new Function<String, WordList>() {
                    @Override
                    public WordList call(String s) throws Exception {
                        WordList result = new WordList();
                        String[] parts = s.split(",");
                        result.setWord(parts[0].trim());
                        result.setWordId(parts[1].trim());
                        return result;
                    }
                }),WordList.class).select("wordId","word");















//        Long worNum = sqlContext.sql("SELECT DISTINCT word FROM counted").count();
//
//        JavaRDD<Row> indx = sqlContext.range(0,worNum).toJavaRDD();
//
//        DataFrame wordList = sqlContext.createDataFrame(sqlContext.sql("SELECT DISTINCT word FROM counted").toJavaRDD()
//                .zipWithIndex().map(new Function<Tuple2<Row, Long>, WordList>() {
//                    @Override
//                    public WordList call(Tuple2<Row, Long> rowLongTuple2) throws Exception {
//                        WordList result = new WordList();
//                        result.setWordId(rowLongTuple2._2().toString().trim());
//                        result.setWord(rowLongTuple2._1().getString(0).trim());
//                        return result;
//                    }
//                }),WordList.class);
//        wordList.registerTempTable("wordList");
//
//        DataFrame wordListSorted = sqlContext.sql("SELECT * FROM wordList ORDER BY wordId").cache();
//        wordListSorted.write().parquet("hdfs://108.108.108.15/user/root/nlp/wordList.parquet");


//        List<Row> rowListSorted = wordListSorted.collectAsList();
//
//        ArrayList<String> wordListLocal = new ArrayList<>();
//
//        for (int i=0;i<rowListSorted.size();i++)
//            wordListLocal.add(rowListSorted.get(i).getString(0).trim());
//
//
//        wordListSorted.show(200);
//        System.out.println(wordListLocal);









        //结果数据:(id,kv)
        JavaPairRDD libSVM = counted.toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String,String> call(Row row)
            {
                return new Tuple2(row.getString(0).trim(),row.getString(1).trim()+","+row.get(2).toString().trim());
            }
        }).groupByKey().mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {

                ArrayList<String> word = new ArrayList<String>();
                ArrayList<String> count = new ArrayList<String>();
                ArrayList<Integer> wordIndx = new ArrayList<>();
                StringBuilder result = new StringBuilder();

                Iterator<String> iter = strings.iterator();
                while (iter.hasNext())
                {
                    String cur = iter.next();
                    String[] parts = cur.split(",");
                    word.add(parts[0].trim());
                    count.add(parts[1].trim());
                }

                for(int i=0;i<word.size();i++)
                    wordIndx.add(broadcastWordList.value().indexOf(word.get(i)));

                for(int i=0;i<word.size()-1;i++)
                    result.append(wordIndx.get(i).toString().trim()+":"+count.get(i).trim()+",");
                result.append(wordIndx.get(word.size()-1).toString().trim()+":"+count.get(word.size()-1).trim());

                //输出格式:(id,wordId:count)
                return result.toString().trim();
            }
        });


        DataFrame parsed = sqlContext.createDataFrame(libSVM.map(new Function<Tuple2<String,String>,Parsed>() {
            @Override
            public Parsed call(Tuple2<String,String> tss) throws Exception {
                Parsed result = new Parsed();
                result.setId(tss._1().trim());
                result.setKv(tss._2().trim());
                return result;
            }
        }),Parsed.class);

        parsed.registerTempTable("parsed");

        DataFrame parsedLibSVM = sqlContext.sql("SELECT " +
                                                    "parsed.id,removed.classLabel,parsed.kv " +
                                                "FROM " +
                                                    "parsed,removed " +
                                                "WHERE " +
                                                    "parsed.id=removed.id");



        //输出格式第一列为文档id(id)，第二列为词id:词频(kv)
//        parsedLibSVM.write().parquet("hdfs://108.108.108.15/user/root/nlp/testParsed.parquet");

        //存储词列表，输出格式：(wordId,word)
//        wordList.write().parquet("hdfs://108.108.108.15/user/root/nlp/testWordList.parquet");

//        parsedLibSVM.show(false);



        jsc.stop();
    }
}
