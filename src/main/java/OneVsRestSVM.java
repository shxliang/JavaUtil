import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author lsx
 * @date 2016/10/8
 */

public class OneVsRestSVM {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("SVM");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame data = sqlContext.read().parquet("hdfs://108.108.108.15/USER/root/nlp/indexed.parquet")
                .select("id", "categoryIndex", "vec").cache();


        List<Row> labelList = data.select("categoryIndex").distinct().collectAsList();
        ArrayList<Double> localLabelList = new ArrayList<>();
        for (int i = 0; i < labelList.size(); i++) {
            localLabelList.add((double) labelList.get(i).get(0));
        }

        final Broadcast<ArrayList<Double>> broadcastLabel = jsc.broadcast(localLabelList);

        final Broadcast<Integer> numClass = jsc.broadcast(localLabelList.size());


        JavaRDD<Tuple2<String, LabeledPoint>> curRDD;
        JavaRDD<LabeledPoint> curLabeledPiont;


        ArrayList<JavaPairRDD<String, Tuple2<Double, Double>>> predictList = new ArrayList<>();

        for (int i = 0; i < numClass.value(); i++) {
            final double curClass = broadcastLabel.value().get(i);

            curRDD = data.toJavaRDD()
                    .map(new Function<Row, Tuple2<String, LabeledPoint>>() {
                        @Override
                        public Tuple2<String, LabeledPoint> call(Row row) throws Exception {
                            if (row.getDouble(1) == curClass) {
                                return new Tuple2<String, LabeledPoint>(row.getString(0).trim(), new LabeledPoint((double) 1, (Vector) row.get(2)));
                            } else {
                                return new Tuple2<String, LabeledPoint>(row.getString(0).trim(), new LabeledPoint((double) 0, (Vector) row.get(2)));
                            }
                        }
                    });


            curLabeledPiont = curRDD.map(new Function<Tuple2<String, LabeledPoint>, LabeledPoint>() {
                @Override
                public LabeledPoint call(Tuple2<String, LabeledPoint> stringLabeledPointTuple2) throws Exception {
                    return stringLabeledPointTuple2._2();
                }
            });


            final SVMModel curModel = SVMWithSGD.train(curLabeledPiont.rdd(), 100);

            JavaRDD<Tuple2<Object, Object>> curPredictions = curLabeledPiont.map(
                    new Function<LabeledPoint, Tuple2<Object, Object>>() {
                        @Override
                        public Tuple2<Object, Object> call(LabeledPoint p) {
                            Double label = curModel.predict(p.features());
                            return new Tuple2<Object, Object>(label, p.label());
                        }
                    }
            );


            MulticlassMetrics curMetrics = new MulticlassMetrics(curPredictions.rdd());

            final Double curPrecision = curMetrics.precision();

            JavaPairRDD<String, Tuple2<Double, Double>> curPredict = curRDD.mapToPair(new PairFunction<Tuple2<String, LabeledPoint>, String, Tuple2<Double, Double>>() {
                @Override
                public Tuple2<String, Tuple2<Double, Double>> call(Tuple2<String, LabeledPoint> stringLabeledPointTuple2) {
                    Double predictResult = curModel.predict(stringLabeledPointTuple2._2().features());
                    if (predictResult == 1.0) {
                        return new Tuple2(stringLabeledPointTuple2._1().trim(), new Tuple2(curClass, curPrecision));
                    } else {
                        return new Tuple2(stringLabeledPointTuple2._1().trim(), new Tuple2(-1.0, curPrecision));
                    }
                }

            });

//            System.out.println(curPredict.collect());

            predictList.add(curPredict);

        }


        JavaPairRDD<String, Tuple2<Double, Double>> predictRDD = predictList.get(0).union(predictList.get(1));
        if (predictList.size() > 2) {
            for (int i = 2; i < predictList.size(); i++) {
                predictRDD = predictRDD.union(predictList.get(i));
            }
        }


        JavaPairRDD<String, Double> predictions = predictRDD.groupByKey().mapValues(new Function<Iterable<Tuple2<Double, Double>>, Double>() {
            @Override
            public Double call(Iterable<Tuple2<Double, Double>> tuple2s) throws Exception {

                ArrayList<Double> label = new ArrayList<Double>();
                ArrayList<Double> prec = new ArrayList<Double>();
                ArrayList<Double> result = new ArrayList<Double>();

                Iterator<Tuple2<Double, Double>> iter = tuple2s.iterator();
                while (iter.hasNext()) {
                    Tuple2 curTuple = iter.next();
                    label.add((Double) curTuple._1());
                    prec.add((Double) curTuple._2());

                }

                for (int i = 0; i < label.size(); i++) {
                    if (label.get(i) != (-1.0)) {
                        result.add(label.get(i));
                    }
                }


                if (result.size() > 1) {
                    double maxLabel = -1;
                    double max = 0;
                    for (int i = 0; i < result.size(); i++) {
                        if (prec.get(label.indexOf(result.get(i))) > max) {
                            max = prec.get(label.indexOf(result.get(i)));
                            maxLabel = result.get(i);
                        }
                    }
                    return maxLabel;

                } else if (result.size() == 1) {
                    return result.get(0);
                } else {
                    return -1.0;
                }


            }
        });


        JavaPairRDD<String, Tuple2<Double, Double>> predicResult = predictions
                .join(data.select("id", "categoryIndex").toJavaRDD().mapToPair(new PairFunction<Row, String, Double>() {
                    @Override
                    public Tuple2<String, Double> call(Row row) {
                        return new Tuple2<String, Double>(row.getString(0).trim(), row.getDouble(1));
                    }
                }));


//        System.out.println(precision);
//        System.out.println(predict);
//        for (JavaPairRDD cur : predict)
//            System.out.println(cur.collect());


//        System.out.println(predicResult.collect());


        JavaRDD<Tuple2<Object, Object>> predicMetrics = predicResult.map(new Function<Tuple2<String, Tuple2<Double, Double>>, Tuple2<Object, Object>>() {
            @Override
            public Tuple2<Object, Object> call(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<Object, Object>(stringTuple2Tuple2._2()._1(), stringTuple2Tuple2._2()._2());
            }
        });

        MulticlassMetrics metrics = new MulticlassMetrics(predicMetrics.rdd());

        System.out.println(metrics.precision());
    }
}
