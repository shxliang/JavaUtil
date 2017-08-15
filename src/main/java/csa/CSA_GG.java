package csa; /**
 * Created by lsx on 2016/9/12.
 */

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;

import java.io.Serializable;

public class CSA_GG {


    public static class RoundPrice implements Serializable
    {
        public String idcode;
        public String date;
        public Integer price;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getDate() {
            return date;
        }

        public void setDate(String date) {
            this.date = date;
        }

        public Integer getPrice() {
            return price;
        }

        public void setPrice(Integer price) {
            this.price = price;
        }
    }



    public static void main(String[] args){

        System.setProperty("spark.executor.memory","10g");
        System.setProperty("spark.worker.memory","10g");
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("test");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);


        DataFrame alldata = sqlContext.read().parquet("hdfs://108.108.108.15/user/root/csair/alldata.parquet");
        alldata.registerTempTable("alldata");


        DataFrame user = sqlContext.read().parquet("hdfs://108.108.108.15/user/root/csair/user.parquet");
        user.registerTempTable("user");


        DataFrame alldata0 = sqlContext.sql("SELECT alldata.*,user.consum FROM alldata,user WHERE alldata.idcode=user.idcode");
        alldata0.registerTempTable("alldata0");


        //贵属广居旅客搭乘记录
        DataFrame ggData = sqlContext.sql("SELECT * FROM alldata0 WHERE idb='贵州' AND resid='CAN'").cache();
        ggData.registerTempTable("ggData");



        //贵属广居旅客用户记录
        DataFrame ggUser = sqlContext.sql("SELECT * FROM user WHERE idb='贵州' AND resid='CAN'").cache();
        ggUser.registerTempTable("ggUser");



//        System.out.println(ggUser.count());


        //贵属广居旅客中同时满足年龄22-45、提前购票3天内、价格不敏感
//        DataFrame gg_all = sqlContext.sql("SELECT * FROM user WHERE (age BETWEEN 22 AND 45) " +
//                                        "AND advan='1' AND idb='贵州' AND resid='CAN' AND consum='nosens'");



//        System.out.println(gg_all.count());

//        DataFrame gg_age = sqlContext.sql("SELECT * FROM ggUser WHERE (age BETWEEN 22 AND 45)");
//        System.out.println(gg_age.count());


//        DataFrame gg_advan = sqlContext.sql("SELECT * FROM ggUser WHERE advan='1'");
//        System.out.println(gg_advan.count());

//        DataFrame gg_consum = sqlContext.sql("SELECT * FROM ggUser WHERE consum='nosens'");
//        System.out.println(gg_consum.count());


//        DataFrame gg_age_advan = sqlContext.sql("SELECT * FROM ggUser WHERE (age BETWEEN 22 AND 45) AND advan='1'");
//        System.out.println(gg_age_advan.count());


//        DataFrame gg_age_consum = sqlContext.sql("SELECT * FROM ggUser WHERE (age BETWEEN 22 AND 45) AND consum='nosens'");
//        System.out.println(gg_age_consum.count());


//        DataFrame gg_advan_consum = sqlContext.sql("SELECT * FROM ggUser WHERE advan='1' AND consum='nosens'");
//        System.out.println(gg_advan_consum.count());







        //贵属广居旅客中航班提起在年三十当天或前一天
//        DataFrame nqtq = sqlContext.sql("SELECT mean(支付价CNY) FROM ggData WHERE 支付价CNY>0 AND 航班日期 IN ('2016-02-06','2016-02-07','2015-02-18','2015-02-19') " +
//                "AND age IN (22,45) AND advan='1' AND consum='nosens' " +
//                "AND 实际始发机场='CAN' AND 实际到达机场='KWE'");
//        nqtq.show();








        //贵属广居旅客中同时够或非同时购的贵-广线支付平均价
//        DataFrame backPrice = sqlContext.sql("SELECT mean(支付价CNY) FROM ggData WHERE 实际始发机场='KWE' AND 实际到达机场='CAN' AND 支付价CNY>0 " +
//                                              "AND round='1' AND age IN (22,45) AND advan='1' AND consum='nosens'");
//        backPrice.show();







        //贵属广居旅客中同时购或非同时购的平均提前天数
//        DataFrame advanDays = sqlContext.sql("SELECT mean(提前购票天数) FROM ggData WHERE 实际始发机场='KWE' AND 实际到达机场='CAN' AND round='0' " +
//                                                "AND age IN (22,45) AND advan='1' AND consum='nosens'");
//        advanDays.show();







//        JavaPairRDD<String,Integer> roundPrice1 = sqlContext.sql("SELECT idcode,支付价CNY,days,fromcity,tocity,出票日期 FROM alldata WHERE round='1'").toJavaRDD()
//                                .mapToPair(new PairFunction<Row, String, String>() {
//                                    @Override
//                                    public Tuple2<String, String> call(Row row) throws Exception {
//                                        String key = row.getString(0).trim()+","+row.getString(5);
//                                        String str;
//                                        if(row.get(1)==null)
//                                            str = "miss";
//                                        else
//                                            str = row.get(1).toString().trim();
//                                        String value = str+","+row.get(2).toString().trim()+","+row.getString(3).trim()+","+row.getString(4).trim();
//                                        return new Tuple2(key,value);
//                                    }
//                                }).groupByKey().mapValues(new Function<Iterable<String>, Integer>() {
//                    @Override
//                    public Integer call(Iterable<String> strings) throws Exception {
//                        ArrayList<Integer> price = new ArrayList<Integer>();
//                        ArrayList<Integer> days = new ArrayList<Integer>();
//                        ArrayList<String> fromcity = new ArrayList<String>();
//                        ArrayList<String> tocity = new ArrayList<String>();
//                        Iterator<String> iter = strings.iterator();
//                        while (iter.hasNext())
//                        {
//                            String cur = iter.next();
//                            String[] parts = cur.split(",");
//                            if (parts[0].equals("miss"))
//                                continue;
//                            price.add(Integer.parseInt(parts[0].trim()));
//                            days.add(Integer.parseInt(parts[1].trim()));
//                            fromcity.add(parts[2]);
//                            tocity.add(parts[3]);
//                        }
//                        if (price.size()<=1)
//                            return null;
//                        int min = 1000;
//                        int indx = -1;
//                        for(int i=0;i<price.size();i++)
//                        {
//                            if (days.get(i)<min)
//                            {
//                                min = days.get(i);
//                                indx = i;
//                            }
//                        }
//                        String from = fromcity.get(indx);
//                        ArrayList<Integer> round_price = new ArrayList<Integer>();
//                        for(int i=0;i<tocity.size();i++)
//                        {
//                            if (tocity.get(i).equals(from))
//                                round_price.add(price.get(i));
//                        }
//
//                        int max =0;
//                        for(int i=0;i<round_price.size();i++)
//                            if (round_price.get(i)>max)
//                                max = round_price.get(i);
//
//                        return max;
//
//                    }
//                });
//        DataFrame roundPrice = sqlContext.createDataFrame(roundPrice1.map(new Function<Tuple2<String, Integer>, RoundPrice>() {
//            @Override
//            public RoundPrice call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                RoundPrice result = new RoundPrice();
//                String[] parts = stringIntegerTuple2._1().split(",");
//                result.setIdcode(parts[0]);
//                result.setDate(parts[1]);
//                result.setPrice(stringIntegerTuple2._2());
//                return result;
//            }
//        }),RoundPrice.class);
//
//        roundPrice.registerTempTable("roundPrice");



//        System.out.println(roundPrice.count());
//        sqlContext.sql("SELECT mean(price) FROM roundPrice WHERE price>0").show();
//        sqlContext.sql("SELECT count(price) FROM roundPrice WHERE price=0").show();









        //1516流失旅客
        DataFrame userInter = sqlContext.sql("SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' " +
                "INTERSECT " +
                "SELECT DISTINCT idcode FROM ggData WHERE 航班年='2016年'").cache();
        userInter.registerTempTable("userInter");
        DataFrame userLost = sqlContext.sql("SELECT DISTINCT idcode FROM ggData " +
                "EXCEPT " +
                "SELECT DISTINCT idcode FROM userInter");
        userLost.registerTempTable("userLost");
        DataFrame dataLost = sqlContext.sql("SELECT ggData.* FROM ggData,userLost WHERE ggData.idcode=userLost.idcode");
        dataLost.show(100);





        //1516重叠旅客
//        DataFrame id15 =sqlContext.sql("SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' AND 实际始发机场='CAN' AND 实际到达机场='KWE' AND round='1'");
//        id15.registerTempTable("id15");
//        DataFrame idInter = sqlContext.sql("SELECT count(DISTINCT ggData.idcode) FROM ggData,id15 WHERE ggData.idcode=id15.idcode " +
//                "AND ggData.航班年='2016年' AND 实际始发机场='CAN' AND 实际到达机场='KWE' AND round='1'");
//        idInter.registerTempTable("idInter");
//        idInter.show();






        //贵-广或广-贵的同时购旅客
//        sqlContext.sql("SELECT count(DISTINCT idcode) FROM ggData WHERE 航班年='2015年' AND round='1' " +
//                "AND (( 实际始发机场='KWE' AND 实际到达机场='CAN' ) OR ( 实际始发机场='CAN' AND 实际到达机场='KWE' ))").show();










        //年前年后重叠的旅客
//        DataFrame inter = sqlContext.sql("SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' AND 实际始发机场='CAN' AND 实际到达机场='KWE' AND days<86 " +
//                "INTERSECT " +
//                "SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' AND 实际始发机场='CAN' AND 实际到达机场='KWE' AND days>=86");
//        inter.show(100);
//        System.out.println(inter.count());

//        sqlContext.sql("SELECT * FROM ggData WHERE idcode='李靖男19810118'").show();







        //贵属广居旅客中没有搭乘贵-广和广-贵的旅客
//        DataFrame setdif = sqlContext.sql("SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' " +
//                "EXCEPT " +
//                "SELECT DISTINCT idcode FROM ggData WHERE 航班年='2015年' AND (( 实际始发机场='KWE' AND 实际到达机场='CAN' ) " +
//                "OR ( 实际始发机场='CAN' AND 实际到达机场='KWE' ))");
//        setdif.show(100);
//        System.out.println(setdif.count());

//        sqlContext.sql("SELECT * FROM ggData WHERE idcode='代志军男19860120'").show();




        jsc.stop();

    }

}

