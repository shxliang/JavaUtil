package textclassifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import util.EvaluateUtil;

/**
 * Created by lsx on 2017/7/12.
 */
public class SparkNaiveBayes {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("SparkMLPC");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);


        String inputPath = "/Users/lsx/Desktop/文本分类";


        DataFrame train = sqlContext.read()
                .parquet(inputPath + "/word2vec_train_200.parquet")
                .cache();
        DataFrame test = sqlContext.read()
                .parquet(inputPath + "/word2vec_test_200.parquet")
                .cache();


        NaiveBayes naiveBayes = new NaiveBayes()
                .setLabelCol("indexedClass")
                .setFeaturesCol("wordVec");
        NaiveBayesModel naiveBayesModel = naiveBayes.fit(train);

        DataFrame result = naiveBayesModel.transform(test);

        EvaluateUtil.EvaluateResult evaluateResult = EvaluateUtil.runEvaluate(result,"prediction","indexedClass");
        System.out.println("Accuracy = " + evaluateResult.getAccuracy());
        System.out.println("F1 = " + evaluateResult.getF1());
    }
}
