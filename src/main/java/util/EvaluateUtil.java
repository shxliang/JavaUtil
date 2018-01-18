package util;

import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.DataFrame;

/**
 * Created by lsx on 2017/7/12.
 */
public class EvaluateUtil {
    public static class EvaluateResult
    {
        double accuracy;
        double f1;

        public double getAccuracy() {
            return accuracy;
        }

        public void setAccuracy(double accuracy) {
            this.accuracy = accuracy;
        }

        public double getF1() {
            return f1;
        }

        public void setF1(double f1) {
            this.f1 = f1;
        }
    }

    public static EvaluateResult runEvaluate(DataFrame inputDF, String predictionCol, String trueLabelCol)
    {
        MulticlassClassificationEvaluator multiclassClassificationEvaluator = new MulticlassClassificationEvaluator()
                .setMetricName("f1")
                .setLabelCol(trueLabelCol)
                .setPredictionCol(predictionCol);
        double f1 = multiclassClassificationEvaluator.evaluate(inputDF);
        double accuracy = (double)inputDF.filter(predictionCol+"="+trueLabelCol).count()/inputDF.count();

        EvaluateResult result = new EvaluateResult();
        result.setAccuracy(accuracy);
        result.setF1(f1);

        return result;
    }
}
