package testclassifier;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel;
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF3;
import org.apache.spark.sql.api.java.UDF5;
import org.apache.spark.sql.types.DataTypes;
import utils.EvaluateUtil;
import utils.MapUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lsx on 2016/9/29.
 */

public class SparkMLPC {
    public static void main(String[] args) throws IOException {
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


        //每层节点数，第一层为输入层，节点数与特征数相等，最后层为输出层，节点数与类别数相等
        List<int[]> layersList = new ArrayList<>();
        layersList.add(new int[]{200, 20, 8});
        layersList.add(new int[]{200, 50, 8});
        layersList.add(new int[]{200, 100, 8});
        layersList.add(new int[]{200, 150, 8});
        layersList.add(new int[]{200, 50, 20, 8});
//        layersList.add(new int[]{200, 150, 50, 8});
//        layersList.add(new int[]{200, 150, 20, 8});
//        layersList.add(new int[]{100, 50, 8});
//        layersList.add(new int[]{100, 20, 8});
//        layersList.add(new int[]{300, 150, 8});

        int index = 0;

        double[] accuracyArray = new double[layersList.size()];
        final double[] f1Array = new double[layersList.size()];

        DataFrame result = test;

        for (int[] curLayers : layersList)
        {
            MultilayerPerceptronClassifier trainer = new MultilayerPerceptronClassifier()
                    .setFeaturesCol("wordVec")
                    .setLabelCol("indexedClass")
                    .setPredictionCol("prediction_"+index)
                    .setLayers(curLayers)
                    .setMaxIter(500);
            MultilayerPerceptronClassificationModel multilayerPerceptronClassificationModel = trainer.fit(train);

            result = multilayerPerceptronClassificationModel.transform(result);

            accuracyArray[index] = (double)result.filter("indexedClass=prediction_"+index).count()/result.count();

            MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                    .setMetricName("f1")
                    .setLabelCol("indexedClass")
                    .setPredictionCol("prediction_"+index);
            f1Array[index] = evaluator.evaluate(result);

            index++;
        }

        result.registerTempTable("result");


        sqlContext.udf().register("bagging3", new UDF3<Double, Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2, Double aDouble3) throws Exception {
                Map<Double,Double> weight = new HashMap<>();
                double[] prediction = new double[]{aDouble, aDouble2, aDouble3};
                for (int i=0;i<prediction.length;i++)
                {
                    double curPre = prediction[i];
                    if (weight.containsKey(curPre))
//                        weight.put(curPre, weight.get(curPre)+f1Array[i]);
                        weight.put(curPre, weight.get(curPre) + 1D);
                    else
//                        weight.put(curPre, f1Array[i]);
                        weight.put(curPre, 1D);
                }
                weight = MapUtil.sortByValue(weight);
                return weight.keySet().iterator().next();
            }
        }, DataTypes.DoubleType);

        sqlContext.udf().register("bagging5", new UDF5<Double, Double, Double, Double, Double, Double>() {
            @Override
            public Double call(Double aDouble, Double aDouble2, Double aDouble3, Double aDouble4, Double aDouble5) throws Exception {
                Map<Double,Double> weight = new HashMap<>();
                double[] prediction = new double[]{aDouble, aDouble2, aDouble3, aDouble4, aDouble5};
                for (int i=0;i<prediction.length;i++)
                {
                    double curPre = prediction[i];
                    if (weight.containsKey(curPre))
//                        weight.put(curPre, weight.get(curPre)+f1Array[i]);
                        weight.put(curPre, weight.get(curPre) + 1D);
                    else
//                        weight.put(curPre, f1Array[i]);
                        weight.put(curPre, 1D);
                }
                weight = MapUtil.sortByValue(weight);
                return weight.keySet().iterator().next();
            }
        }, DataTypes.DoubleType);


        result = sqlContext.sql("SELECT indexedClass," +
                "prediction_0 AS prediction " +
                "FROM result");
//        result = sqlContext.sql("SELECT indexedClass," +
//                "bagging3(prediction_0,prediction_1,prediction_2) AS prediction " +
//                "FROM result");
//        result = sqlContext.sql("SELECT indexedClass," +
//                "bagging5(prediction_0,prediction_1,prediction_2,prediction_3,prediction_4) AS prediction " +
//                "FROM result");


        EvaluateUtil.EvaluateResult evaluateResult = EvaluateUtil.runEvaluate(result,"prediction","indexedClass");
        double accuracy = evaluateResult.getAccuracy();
        double f1 = evaluateResult.getF1();

        System.out.println("Accuracy = " + accuracy);
        System.out.println("F1 = " + f1);

        jsc.stop();
    }
}
