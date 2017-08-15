package testclassifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.feature.RegexTokenizer;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.Word2Vec;
import org.apache.spark.ml.feature.Word2VecModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.io.IOException;

/**
 * Created by lsx on 2017/7/12.
 */
public class SparkWord2Vec {
    public static void main(String[] args) throws IOException {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("word2vec");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);


        String inputPath = "/Users/lsx/Desktop/文本分类";

        DataFrame documentDF = sqlContext.read().parquet(inputPath + "/removed.parquet");
        RegexTokenizer regexTokenizer = new RegexTokenizer()
                .setInputCol("segmentedWords")
                .setOutputCol("words")
                .setPattern(" ");
        DataFrame wordsDF = regexTokenizer.transform(documentDF);
        StringIndexer indexer = new StringIndexer()
                .setInputCol("class")
                .setOutputCol("indexedClass");
        DataFrame indexed = indexer.fit(wordsDF)
                .transform(wordsDF);

        DataFrame[] parts = indexed.randomSplit(new double[]{0.8,0.2},1231);
        DataFrame train = parts[0];
        DataFrame test = parts[1];

        Word2Vec word2Vec = new Word2Vec()
                .setInputCol("words")
                .setOutputCol("wordVec")
                .setVectorSize(200)
                .setMinCount(0);
        Word2VecModel word2VecModel = word2Vec.fit(train);

        saveModel(word2VecModel,inputPath+"/word2vec.model");

        train = word2VecModel.transform(train)
                .select("docId","indexedClass","wordVec");
        test = word2VecModel.transform(test)
                .select("docId","indexedClass","wordVec");

        train.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(inputPath + "/word2vec_train_300.parquet");
        test.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(inputPath + "/word2vec_test_300.parquet");

    }

    private static void saveModel(Word2VecModel word2VecModel,String path) throws IOException {
        word2VecModel.write()
                .overwrite()
                .save(path);
    }
}
