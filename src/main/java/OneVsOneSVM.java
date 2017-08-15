//import org.apache.spark.SparkConf;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.Function;
//import org.apache.spark.api.java.function.PairFunction;
//import org.apache.spark.broadcast.Broadcast;
//import org.apache.spark.ml.feature.StringIndexer;
//import org.apache.spark.mllib.textclassifier.SVMModel;
//import org.apache.spark.mllib.textclassifier.SVMWithSGD;
//import org.apache.spark.mllib.evaluation.MulticlassMetrics;
//import org.apache.spark.mllib.linalg.Vector;
//import org.apache.spark.mllib.linalg.Vectors;
//import org.apache.spark.mllib.regression.LabeledPoint;
//import org.apache.spark.sql.DataFrame;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SQLContext;
//import scala.Tuple2;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//
///**
// * Created by lsx on 2016/10/13.
// */
//public class OneVsOneSVM {
//    public static void main(String[] args)
//    {
//        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("OneVsOneSVM");
//        JavaSparkContext jsc = new JavaSparkContext(sc);
//        SQLContext sqlContext = new SQLContext(jsc);
//
//
//        DataFrame data = sqlContext.read()
//                .format("com.databricks.spark.csv")
//                .option("inferSchema", "true")
//                .option("header", "true")
//                .load("E:\\iris.csv");
//
//        //结果数据，(label,features)
//        JavaPairRDD<String,Vector> dataRDD = data.toJavaRDD().mapToPair(new PairFunction<Row, String, Vector>() {
//            @Override
//            public Tuple2<String, Vector> call(Row row) throws Exception {
//                double[] pionts = new double[row.size()-1];
//                for (int i=0;i<pionts.length;i++)
//                    pionts[i] = row.getDouble(i+1);
//                return new Tuple2<String, Vector>(row.get(0).toString(), Vectors.dense(pionts));
//            }
//        });
//
//
//
//
//        //增加序号列，并转化为DataFrame
//        DataFrame dataDF = sqlContext.createDataFrame(dataRDD.zipWithIndex().map(new Function<Tuple2<Tuple2<String, Vector>, Long>, TrueTest.Labeled>() {
//            @Override
//            public TrueTest.Labeled call(Tuple2<Tuple2<String, Vector>, Long> tuple2LongTuple2) throws Exception {
//                TrueTest.Labeled result = new TrueTest.Labeled();
//                result.setClassLabel(tuple2LongTuple2._1()._1().trim());
//                result.setVec(tuple2LongTuple2._1()._2());
//                result.setId(tuple2LongTuple2._2().toString().trim());
//                return result;
//            }
//        }), TrueTest.Labeled.class);
//
//
//        StringIndexer labelIndexer = new StringIndexer()
//                .setInputCol("classLabel")
//                .setOutputCol("indexedLabel");
//        DataFrame indexed = labelIndexer.fit(dataDF).transform(dataDF).select("id","indexedLabel","vec").cache();
//
//
//        List<Row> labelList = indexed.select("indexedLabel").distinct().collectAsList();
//        ArrayList<Double> localLabelList = new ArrayList<>();
//
//        for (int i=0;i<labelList.size();i++)
//            localLabelList.add((double)labelList.get(i).get(0));
//
//        //类别列表
//        final Broadcast<ArrayList<Double>> broadcastLabel = jsc.broadcast(localLabelList);
//
//        //类别数
//        final Broadcast<Integer> numClass = jsc.broadcast(localLabelList.size());
//
//
//        JavaRDD<Tuple2<String,LabeledPoint>> curRDD;
//        JavaRDD<LabeledPoint> curLabeledPiont;
//
//
//        ArrayList<JavaPairRDD<String,Tuple2<Double,Double>>> predictList = new ArrayList<>();
//
//
//        for (int i=0;i<numClass.value()-1;i++)
//            for (int j=i+1;j<numClass.value();j++)
//            {
//                double curClass1 = broadcastLabel.value().get(i);
//                double curClass2 = broadcastLabel.value().get(j);
//
//                curRDD = indexed.toJavaRDD()
//                        .map(new Function<Row, Tuple2<String,LabeledPoint>>() {
//                            @Override
//                            public Tuple2<String,LabeledPoint> call(Row row) throws Exception {
//                                if (row.getDouble(1) == curClass1)
//                                    //返回值:(id,(label,features))
//                                    return new Tuple2<String, LabeledPoint>(row.getString(0).trim(),new LabeledPoint((double)0,(Vector)row.get(2)));
//                                if (row.getDouble(1) == curClass2)
//                                    return new Tuple2<String, LabeledPoint>(row.getString(0).trim(),new LabeledPoint((double)1,(Vector)row.get(2)));
//                                return new Tuple2<String, LabeledPoint>(row.getString(0).trim(),new LabeledPoint((double)-1,(Vector)row.get(2)));
//                            }
//                        }).filter(new Function<Tuple2<String, LabeledPoint>, Boolean>() {
//                            @Override
//                            public Boolean call(Tuple2<String, LabeledPoint> stringLabeledPointTuple2) throws Exception {
//                                if (stringLabeledPointTuple2._2().label() == -1.0)
//                                    return false;
//                                else
//                                    return true;
//                            }
//                        });
//
//
//
//                curLabeledPiont = curRDD.map(new Function<Tuple2<String, LabeledPoint>, LabeledPoint>() {
//                    @Override
//                    public LabeledPoint call(Tuple2<String, LabeledPoint> stringLabeledPointTuple2) throws Exception {
//                        return stringLabeledPointTuple2._2();
//                    }
//                });
//
//
//                SVMModel curModel = SVMWithSGD.train(curLabeledPiont.rdd(), 1000);
//
//                JavaRDD<Tuple2<Object, Object>> curPredictions = curLabeledPiont.map(
//                        new Function<LabeledPoint, Tuple2<Object, Object>>() {
//                            public Tuple2<Object, Object> call(LabeledPoint p) {
//                                Double label = curModel.predict(p.features());
//                                //返回值:(预测值,真实值)
//                                return new Tuple2<Object, Object>(label, p.label());
//                            }
//                        }
//                );
//
//
//                MulticlassMetrics curMetrics = new MulticlassMetrics(curPredictions.rdd());
//
//                Double curPrecision = curMetrics.precision();
//
//                JavaPairRDD<String,Tuple2<Double,Double>> curPredict = curRDD.mapToPair(new PairFunction<Tuple2<String,LabeledPoint>, String, Tuple2<Double,Double>>() {
//                    public Tuple2<String,Tuple2<Double,Double>> call(Tuple2<String,LabeledPoint> stringLabeledPointTuple2)
//                    {
//                        Double predictResult = curModel.predict(stringLabeledPointTuple2._2().features());
//                        if (predictResult == 1.0)
//                            //返回值:(id,(预测值,正确率))
//                            return new Tuple2(stringLabeledPointTuple2._1().trim(),new Tuple2(curClass2,curPrecision));
//                        else
//                            return new Tuple2(stringLabeledPointTuple2._1().trim(),new Tuple2(curClass1,curPrecision));
//                    }
//
//                });
//
////            System.out.println(curPredict.collect());
//
//                predictList.add(curPredict);
//
//            }
//
//
//
//
////        for (int i=0;i<predictList.size();i++)
////            System.out.println(predictList.get(i).collect());
//
//
//
//
//
//        //合并各二分类结果
//        JavaPairRDD<String,Tuple2<Double,Double>> predictRDD = predictList.get(0).union(predictList.get(1));
//        if (predictList.size()>2)
//            for (int i=2;i<predictList.size();i++)
//                predictRDD = predictRDD.union(predictList.get(i));
//
//
//
//        //结果数据:(id,预测值)
//        JavaPairRDD<String,Double> predictions = predictRDD.groupByKey().mapValues(new Function<Iterable<Tuple2<Double, Double>>, Double>() {
//            @Override
//            //输入数据:(预测值,正确率)
//            public Double call(Iterable<Tuple2<Double, Double>> tuple2s) throws Exception {
//
//                ArrayList<Double> label = new ArrayList<Double>();
//                ArrayList<Integer> count = new ArrayList<Integer>();
//                ArrayList<Double> prec = new ArrayList<Double>();
//                ArrayList<Double> result = new ArrayList<Double>();
//
//                Iterator<Tuple2<Double,Double>> iter = tuple2s.iterator();
//                while (iter.hasNext())
//                {
//                    Tuple2<Double,Double> curTuple = iter.next();
//                    if (!label.contains(curTuple._1()))
//                    {
//                        label.add(curTuple._1());
//                        prec.add(curTuple._2());
//                        count.add(1);
//                    }
//                    else
//                    {
//                        count.set(label.indexOf(curTuple._1()),count.get(label.indexOf(curTuple._1()))+1);
//                        prec.set(label.indexOf(curTuple._1()),prec.get(label.indexOf(curTuple._1()))+curTuple._2());
//
//                    }
//
//                }
//
//                for (int i=0;i<prec.size();i++)
//                    prec.set(i,prec.get(i)/count.get(i));
//
//                Integer maxCount = 0;
//                for (int i=0;i<count.size();i++)
//                    maxCount = Math.max(maxCount,count.get(i));
//
//                for (int i=0;i<label.size();i++)
//                    if (count.get(i) == maxCount)
//                        result.add(label.get(i));
//
//                if (result.size() == 1)
//                    return result.get(0);
//                else
//                {
//                    Double maxPrec = 0.0;
//                    int maxIndex = -1;
//                    for (int i=0;i<result.size();i++)
//                    {
//                        if (prec.get(label.indexOf(result.get(i))) > maxPrec)
//                        {
//                            maxPrec = Math.max(maxPrec,prec.get(label.indexOf(result.get(i))));
//                            maxIndex = i;
//                        }
//
//                    }
//
//                    return result.get(maxIndex);
//
//                }
//
//            }
//        });
//
//
//
//
//        //将预测值与真实值合并，(id,(预测值,真实值))
//        JavaPairRDD<String,Tuple2<Double,Double>> predicResult = predictions
//                .join(indexed.select("id","indexedLabel").toJavaRDD().mapToPair(new PairFunction<Row, String, Double>() {
//                    public Tuple2<String,Double> call(Row row)
//                    {
//                        return new Tuple2<String, Double>(row.getString(0).trim(),row.getDouble(1));
//                    }
//                }));
//
//
//
////        System.out.println(predicResult.collect());
//
//
//
//
//        //只提取预测值和真实值用于评估，(预测值,真实值)
//        JavaRDD<Tuple2<Object,Object>> predicMetrics = predicResult.map(new Function<Tuple2<String, Tuple2<Double, Double>>, Tuple2<Object, Object>>() {
//            @Override
//            public Tuple2<Object, Object> call(Tuple2<String, Tuple2<Double, Double>> stringTuple2Tuple2) throws Exception {
//                return new Tuple2<Object, Object>(stringTuple2Tuple2._2()._1(),stringTuple2Tuple2._2()._2());
//            }
//        });
//
//
//        MulticlassMetrics metrics = new MulticlassMetrics(predicMetrics.rdd());
//
//
//        System.out.println(metrics.precision());
//
//
//
//    }
//
//}
