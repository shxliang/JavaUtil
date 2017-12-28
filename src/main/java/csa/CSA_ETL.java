package csa;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

/**
 * Created by lsx on 2016/9/8.
 */
public class CSA_ETL {



    public static class Channel implements Serializable
    {
        public String idcode;
        public String ticketChannel;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }
        public String getTicketChannel() {
            return ticketChannel;
        }

        public void setTicketChannel(String ticketChannel) {
            this.ticketChannel = ticketChannel;
        }
    }

    public static class Pay implements Serializable
    {
        public String idcode;
        public String paymentMode;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getPaymentMode() {
            return paymentMode;
        }

        public void setPaymentMode(String paymentMode) {
            this.paymentMode = paymentMode;
        }



    }

    public static class Vip implements Serializable
    {
        public String idcode;
        public String vip;


        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getVip() {
            return vip;
        }

        public void setVip(String vip) {
            this.vip = vip;
        }
    }


    public static class Group implements Serializable
    {
        public String idcode;
        public String groupMark;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getGroupMark() {
            return groupMark;
        }

        public void setGroupMark(String groupMark) {
            this.groupMark = groupMark;
        }
    }


    public static class Age implements Serializable
    {
        public String idcode;
        public Integer age;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }




    public static class Advan implements Serializable
    {
        public String idcode;
        public String advan;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getAdvan() {
            return advan;
        }

        public void setAdvan(String advan) {
            this.advan = advan;
        }
    }



    public static class Count implements Serializable
    {
        public String idcode;
        public Integer count;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }


    public static class Freque implements Serializable
    {
        public String idcode;
        public String freque;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getFreque() {
            return freque;
        }

        public void setFreque(String freque) {
            this.freque = freque;
        }
    }



    public static class Resid implements Serializable
    {
        public String idcode;
        public String resid;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getResid() {
            return resid;
        }

        public void setResid(String resid) {
            this.resid = resid;
        }
    }


    public static class Partner implements Serializable
    {
        public String idcode;
        public String partner;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getPartner() {
            return partner;
        }

        public void setPartner(String partner) {
            this.partner = partner;
        }
    }

    public static class Consum implements Serializable
    {
        public String idcode;
        public String consum;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getConsum() {
            return consum;
        }

        public void setConsum(String consum) {
            this.consum = consum;
        }
    }


    public static class Idb implements Serializable
    {
        public String idcode;
        public String idb;

        public String getIdcode() {
            return idcode;
        }

        public void setIdcode(String idcode) {
            this.idcode = idcode;
        }

        public String getIdb() {
            return idb;
        }

        public void setIdb(String idb) {
            this.idb = idb;
        }
    }

    public static class Round implements Serializable
    {
        public String idcode;
        public String date;
        public String round;

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

        public String getRound() {
            return round;
        }

        public void setRound(String round) {
            this.round = round;
        }
    }








    public static void main(String[] arg) throws Exception
    {
        SparkConf sparkConf = new SparkConf().setMaster("local[*]").setAppName("csa.CSA_ETL");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(jsc);



        DataFrame df15 = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("hdfs://108.108.108.15/USER/root/csair/ET报表_2015.01.01~2015.03.31.csv");
        DataFrame df16 = sqlContext.read()
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load("hdfs://108.108.108.15/USER/root/csair/ET报表_2016.01.01~2016.03.31.csv");
        DataFrame alld = df15.unionAll(df16);
        alld.registerTempTable("alld");





        //定义SQL UDF
        sqlContext.udf().register("myname", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception
            {
                if(s.isEmpty()) {
                    return "@";
                } else {
                    return s.trim();
                }
            }
        },DataTypes.StringType);

        sqlContext.udf().register("mysex", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception
            {
                if(s.isEmpty()) {
                    return "#";
                } else {
                    return s.trim();
                }
            }
        },DataTypes.StringType);

        sqlContext.udf().register("mybirth", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception
            {
                if(s.isEmpty()) {
                    return "*";
                } else {
                    return s.trim();
                }
            }
        },DataTypes.StringType);


        sqlContext.udf().register("myvip", new UDF1<Long, String>() {
            @Override
            public String call(Long l) throws Exception
            {
                if(l==null) {
                    return "1";
                } else {
                    return "0";
                }
            }

        }, DataTypes.StringType);

        sqlContext.udf().register("myfrom", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception
            {
                String[] parts=s.split("-");
                return parts[0].trim();
            }
        },DataTypes.StringType);

        sqlContext.udf().register("myto", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception
            {
                String[] parts=s.split("-");
                return parts[1].trim();
            }
        },DataTypes.StringType);


        sqlContext.udf().register("myadvan", new UDF1<Integer, String>() {
            @Override
            public String call(Integer i) throws Exception
            {

                if (i==null) {
                    return null;
                }

                if(i <= 3) {
                    return "1";
                } else {
                    return "0";
                }
            }
        },DataTypes.StringType);


        sqlContext.udf().register("myage", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s) throws Exception
            {
                try {
                    Integer year = Integer.parseInt(s.trim().substring(0,4));
                    Integer result = 2016-year;
                    if (result <=130 && result >0) {
                        return result;
                    } else {
                        return null;
                    }
                }
                catch (Exception e)
                {
                    return null;
                }
            }
        },DataTypes.IntegerType);


        sqlContext.udf().register("mydays", new UDF1<String, Integer>() {
            @Override
            public Integer call(String s)
            {
                String[] parts = s.split("-");
                try {
                    Integer result = Integer.parseInt(parts[1].trim())*31+Integer.parseInt(parts[2].trim());
                    return result;
                }catch (Exception e)
                {
                    return null;
                }
            }
        },DataTypes.IntegerType);




        sqlContext.udf().register("myidb", new UDF1<String, String>() {
            @Override
            public String call(String s)
            {
                if (s.length()==0) {
                    return null;
                }
                String[] temp = s.split("自治区");
                if (temp.length==1 && temp[0].equals(s)) {
                    temp = s.split("省");
                }
                if (temp.length==1 && temp[0].equals(s)) {
                    temp = s.split("市");
                }
                return temp[0].trim();
            }
        },DataTypes.StringType);


        sqlContext.udf().register("mybucket", new UDF1<Integer, String>() {
            @Override
            public String call(Integer i)
            {

                if (i == null) {
                    return null;
                }
                String result;
                if (i<=3) {
                    result = "1";
                } else if (i>3 && i<=7) {
                    result = "2";
                } else {
                    result = "3";
                }
                return result;
            }
        },DataTypes.StringType);







        DataFrame alldata1 = sqlContext.sql("SELECT *," +
                "CONCAT(myname(旅客姓名),mysex(旅客性别),mybirth(旅客生日)) AS idcode," +
                "myvip(会员卡号) AS vip," +
                "myfrom(OD) AS fromcity," +
                "myto(OD) AS tocity," +
                "myadvan(提前购票天数) AS advan," +
                "myage(旅客生日) AS age," +
                "mydays(航班日期) AS days," +
                "myidb(身份证属地) AS idb," +
                "mybucket(提前购票天数) AS bucket " +
                "FROM alld");



        alldata1.registerTempTable("alldata1");



        JavaPairRDD<String,String> resid1 = alldata1.select("idcode","fromcity","tocity","days","航班年").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String str = "null";
                        if (row.get(3) == null) {
                            str = "miss";
                        } else {
                            str = row.get(3).toString().trim();
                        }

                        String temp = row.getString(1).trim()+","+row.getString(2).trim()+","+str+","+row.getString(4).trim();
                        return new Tuple2(row.getString(0).trim(),temp);
                    }
                }
        ).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s+","+s2;
            }
        });


        JavaPairRDD<String,String> resid2 = resid1.mapValues(new Function<String, String>() {
            @Override
            public String call(String str) throws Exception {


                ArrayList<String> fromcity = new ArrayList<String>();
                ArrayList<String> tocity = new ArrayList<String>();
                ArrayList<Integer> days = new ArrayList<Integer>();
                ArrayList<String> year = new ArrayList<String>();


                String[] parts = str.split(",");
                for (int i =0;i<parts.length;i=i+4)
                {
                    fromcity.add(parts[i].trim());
                    tocity.add(parts[i+1].trim());
                    try {
                        days.add(Integer.parseInt(parts[i+2]));
                    }catch (Exception e)
                    {
                        days.add(null);
                    }

                    year.add(parts[i+3].trim());
                }

                Integer min = 10000;
                int res_indx = -1;
                for (int i=0;i<days.size();i++)
                {
                    if (days.get(i)==null) {
                        continue;
                    }
                    if(days.get(i)<min)
                    {
                        min = days.get(i);
                        res_indx = i;
                    }
                }
                if (res_indx == -1) {
                    return null;
                }
                if (year.get(res_indx).equals("2015年"))
                {
                    if(days.get(res_indx) < 86) {
                        return fromcity.get(res_indx);
                    } else {
                        return tocity.get(res_indx);
                    }
                }
                else
                {
                    if (days.get(res_indx) < 75) {
                        return fromcity.get(res_indx);
                    } else {
                        return tocity.get(res_indx);
                    }

                }
            }
        });



        DataFrame resid = sqlContext.createDataFrame(resid2.map(new Function<Tuple2<String, String>, Resid>() {
            @Override
            public Resid call(Tuple2<String, String> tss) throws Exception {
                Resid result = new Resid();
                result.setIdcode(tss._1().trim());
                result.setResid(tss._2());
                return result;
            }
        }),Resid.class);

        resid.registerTempTable("resid");



        DataFrame alldata2 = sqlContext.sql("SELECT alldata1.*,resid FROM alldata1,resid WHERE alldata1.idcode=resid.idcode");
        alldata2.registerTempTable("alldata2");

        JavaPairRDD<String,String> round1 = sqlContext.sql("SELECT idcode,出票日期,fromcity,tocity,days FROM alldata2").toJavaRDD()
                .mapToPair(new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String key = row.getString(0).trim()+","+row.getString(1).trim();
                        String value = row.getString(2).trim()+","+row.getString(3).trim()+","+row.get(4).toString().trim();
                        return new Tuple2(key,value);
                    }
                }).groupByKey().mapValues(new Function<Iterable<String>, String>() {
                    @Override
                    public String call(Iterable<String> strings) throws Exception {
                        ArrayList<String> fromcity = new ArrayList<String>();
                        ArrayList<String> tocity = new ArrayList<String>();
                        ArrayList<Integer> days = new ArrayList<Integer>();
                        Iterator<String> iter = strings.iterator();
                        while (iter.hasNext())
                        {
                            String[] parts = iter.next().split(",");
                            fromcity.add(parts[0]);
                            tocity.add(parts[1]);
                            days.add(Integer.parseInt(parts[2].trim()));
                        }
                        int min = 10000;
                        int indx = -1;
                        for(int i=0;i<days.size();i++)
                        {
                            if(days.get(i)<min)
                            {
                                min = days.get(i);
                                indx = i;
                            }
                        }
                        String from = fromcity.get(indx);
                        for(int i=0;i<tocity.size();i++)
                        {
                            if(from.equals(tocity.get(i))) {
                                return "1";
                            }
                        }
                        return "0";
                    }
                });

        DataFrame round = sqlContext.createDataFrame(round1.map(new Function<Tuple2<String, String>, Round>() {
            @Override
            public Round call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Round result = new Round();
                String[] parts = stringStringTuple2._1().split(",");
                result.setIdcode(parts[0].trim());
                result.setDate(parts[1].trim());
                result.setRound(stringStringTuple2._2().trim());
                return result;
            }
        }),Round.class);

        round.registerTempTable("round");

//        DataFrame alldata = sqlContext.sql("SELECT alldata2.*,round FROM alldata2,round " +
//                                            "WHERE alldata2.idcode=round.idcode AND alldata2.出票日期=round.date").cache();

        DataFrame alldata = sqlContext.read().parquet("hdfs://108.108.108.15/USER/root/csair/alldata_new.parquet").cache();
//       DataFrame alldata = sqlContext.read().format("com.databricks.spark.csv").option("schema","true").option("header","true").load("hdfs://108.108.108.15/USER/root/csair/alldata.csv").cache();


        alldata.registerTempTable("alldata");













//        alldata.write().parquet("hdfs://108.108.108.15/USER/root/csair/alldata_new.parquet");

//        alldata.write().format("com.databricks.spark.csv").option("header","true").save("hdfs://108.108.108.15/USER/root/csair/alldata.csv");

//        alldata.printSchema();









//        DataFrame temp15 = sqlContext.sql("SELECT OD,fromcity,tocity,旅客人数 FROM alldata WHERE 航班年='2015年'");
//        temp15.registerTempTable("temp15");
//        DataFrame temp16 = sqlContext.sql("SELECT OD,fromcity,tocity,旅客人数 FROM alldata WHERE 航班年='2016年'");
//        temp16.registerTempTable("temp16");
//        DataFrame od15 = sqlContext.sql("SELECT OD,sum(旅客人数) AS num15 FROM temp15 GROUP BY OD");
//        DataFrame od16 = sqlContext.sql("SELECT OD AS OD16,sum(旅客人数) AS num16 FROM temp16 GROUP BY OD");
//        DataFrame od = od15.join(od16,od15.col("OD").equalTo(od16.col("OD16")),"inner");
//        od.registerTempTable("od");
//        DataFrame od_res = sqlContext.sql("SELECT OD,num16-num15 AS dif FROM od");
//        DataFrame od_asc = od_res.sort("dif").limit(10);
//        DataFrame od_des = od_res.sort(org.apache.spark.sql.functions.desc("dif")).limit(10);
//        od_asc.write().parquet("hdfs://108.108.108.15/USER/root/csair/od_asc_top10.parquet");
//        od_des.write().parquet("hdfs://108.108.108.15/USER/root/csair/od_des_top10.parquet");
//
//        DataFrame fromcity15 = sqlContext.sql("SELECT fromcity,sum(旅客人数) AS num15 FROM temp15 GROUP BY fromcity");
//        DataFrame fromcity16 = sqlContext.sql("SELECT fromcity AS fromcity16,sum(旅客人数) AS num16 FROM temp16 GROUP BY fromcity");
//        DataFrame fromcity = fromcity15.join(fromcity16,fromcity15.col("fromcity").equalTo(fromcity16.col("fromcity16")),"inner");
//        fromcity.registerTempTable("fromcity");
//        DataFrame fromcity_res = sqlContext.sql("SELECT fromcity,num16-num15 AS dif FROM fromcity");
//        DataFrame fromcity_asc = fromcity_res.sort("dif").limit(10);
//        DataFrame fromcity_des = fromcity_res.sort(org.apache.spark.sql.functions.desc("dif")).limit(10);
//        fromcity_asc.write().parquet("hdfs://108.108.108.15/USER/root/csair/fromcity_asc_top10.parquet");
//        fromcity_des.write().parquet("hdfs://108.108.108.15/USER/root/csair/fromcity_des_top10.parquet");
//
//        DataFrame tocity15 = sqlContext.sql("SELECT tocity,sum(旅客人数) AS num15 FROM temp15 GROUP BY tocity");
//        DataFrame tocity16 = sqlContext.sql("SELECT tocity AS tocity16,sum(旅客人数) AS num16 FROM temp16 GROUP BY tocity");
//        DataFrame tocity = tocity15.join(tocity16,tocity15.col("tocity").equalTo(tocity16.col("tocity16")),"inner");
//        tocity.registerTempTable("tocity");
//        DataFrame tocity_res = sqlContext.sql("SELECT tocity,num16-num15 AS dif FROM tocity");
//        DataFrame tocity_asc = tocity_res.sort("dif").limit(10);
//        DataFrame tocity_des = tocity_res.sort(org.apache.spark.sql.functions.desc("dif")).limit(10);
//        tocity_asc.write().parquet("hdfs://108.108.108.15/USER/root/csair/tocity_asc_top10.parquet");
//        tocity_des.write().parquet("hdfs://108.108.108.15/USER/root/csair/tocity_des_top10.parquet");







        //按idcode分组，计算出票渠道众数
        JavaPairRDD<String,Iterable<String>> ticketChannel1 = alldata.select("idcode","出票渠道").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2(row.getString(0).trim(),row.getString(1).trim());
                    }
                }
        ).groupByKey();

        JavaPairRDD<String,String> ticketChannel2 = ticketChannel1.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> istr) throws Exception {
                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                Iterator<String> iter = istr.iterator();
                while(iter.hasNext())
                {
                    String cur = iter.next();
                    if(cur == null) {
                        continue;
                    }
                    if(str.indexOf(cur) < 0)
                    {
                        str.add(cur);
                        num.add(1);
                    }
                    else
                    {
                        int indx = str.indexOf(cur);
                        num.set(indx,num.get(indx)+1);
                    }
                }
                if (str.size() == 0) {
                    return null;
                }
                int max = 0;
                int res_indx =0;
                for(int i=0;i<num.size();i++)
                {
                    if(max < num.get(i))
                    {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return str.get(res_indx);
            }
        });

        DataFrame ticketChannel = sqlContext.createDataFrame(ticketChannel2.map(new Function<Tuple2<String, String>, Channel>() {
            @Override
            public Channel call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Channel result = new Channel();
                result.setIdcode(stringStringTuple2._1().trim());
                result.setTicketChannel(stringStringTuple2._2());
                return result;
            }
        }),Channel.class);

        ticketChannel.registerTempTable("ticketChannel");














        JavaPairRDD<String,Iterable<String>> paymentMode1 = alldata.select("idcode","支付方式").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2(row.getString(0).trim(),row.getString(1));
                    }
                }
        ).groupByKey();

        JavaPairRDD<String,String> paymentMode2 = paymentMode1.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> istr) throws Exception {
                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                Iterator<String> iter = istr.iterator();
                while(iter.hasNext())
                {
                    String cur = iter.next();
                    if(cur == null) {
                        continue;
                    }
                    String[] parts = cur.split(";");
                    for(int i=0;i<parts.length;i++)
                    {
                        if (str.indexOf(parts[i]) < 0) {
                            str.add(parts[i].trim());
                            num.add(1);
                        } else {
                            int indx = str.indexOf(parts[i]);
                            num.set(indx, num.get(indx) + 1);
                        }
                    }
                }
                if(str.size() == 0) {
                    return null;
                }
                int max = 0;
                int res_indx =0;
                for(int i=0;i<num.size();i++)
                {
                    if(max < num.get(i))
                    {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return str.get(res_indx);
            }
        });

        DataFrame paymentMode = sqlContext.createDataFrame(paymentMode2.map(new Function<Tuple2<String, String>, Pay>() {
            @Override
            public Pay call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Pay result = new Pay();
                result.setIdcode(stringStringTuple2._1().trim());
                result.setPaymentMode(stringStringTuple2._2());
                return result;
            }
        }),Pay.class);

        paymentMode.registerTempTable("paymentMode");











        JavaPairRDD<String,Iterable<String>> vip1 = alldata.select("idcode","vip").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2(row.getString(0).trim(),row.getString(1).trim());
                    }
                }
        ).groupByKey();

        JavaPairRDD<String,String> vip2 = vip1.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> istr) throws Exception {
                Iterator<String> iter = istr.iterator();
                Integer max = 0;
                while (iter.hasNext())
                {
                    Integer cur = Integer.parseInt(iter.next().trim());
                    if(cur > max) {
                        max = cur;
                    }
                }
                return max.toString();
            }
        });

        DataFrame vip = sqlContext.createDataFrame(vip2.map(new Function<Tuple2<String, String>, Vip>() {
            @Override
            public Vip call(Tuple2<String, String> tss) throws Exception {
                Vip result = new Vip();
                result.setIdcode(tss._1().trim());
                result.setVip(tss._2());
                return result;
            }
        }),Vip.class);

        vip.registerTempTable("vip");
















        JavaPairRDD<String,Iterable<String>> groupMark1 = alldata.select("idcode","团散标识").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2(row.getString(0).trim(),row.getString(1));
                    }
                }
        ).groupByKey();


        JavaPairRDD<String,String> groupMark2 = groupMark1.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> istr) throws Exception {
                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                Iterator<String> iter = istr.iterator();
                while(iter.hasNext())
                {
                    String cur = iter.next();
                    if(cur == null) {
                        continue;
                    }
                    if(str.indexOf(cur) < 0)
                    {
                        str.add(cur);
                        num.add(1);
                    }
                    else
                    {
                        int indx = str.indexOf(cur);
                        num.set(indx,num.get(indx)+1);
                    }
                }
                if(str.size() == 0) {
                    return null;
                }
                int max = 0;
                int res_indx =0;
                for(int i=0;i<num.size();i++)
                {
                    if(max < num.get(i))
                    {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return str.get(res_indx);
            }
        });

        DataFrame groupMark = sqlContext.createDataFrame(groupMark2.map(new Function<Tuple2<String, String>, Group>() {
            @Override
            public Group call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Group result = new Group();
                result.setIdcode(stringStringTuple2._1().trim());
                result.setGroupMark(stringStringTuple2._2());
                return result;
            }
        }),Group.class);

        groupMark.registerTempTable("groupMark");













        JavaPairRDD<String,Iterable<Integer>> age1 = alldata.select("idcode","age").toJavaRDD().mapToPair(
                new PairFunction<Row, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Row row) throws Exception {
                        try {
                            Integer age = row.getInt(1);
                            return new Tuple2(row.getString(0).trim(),age);
                        }
                        catch (Exception e)
                        {
                            Integer age = null;
                            return new Tuple2(row.getString(0).trim(),age);
                        }
                    }
                }
        ).groupByKey();

        JavaPairRDD<String,Integer> age2 = age1.mapValues(new Function<Iterable<Integer>, Integer>() {
            @Override
            public Integer call(Iterable<Integer> istr) throws Exception {
                Iterator<Integer> iter = istr.iterator();
                int max = 0;
                while (iter.hasNext())
                {
                    Integer cur = iter.next();
                    if(cur == null) {
                        continue;
                    }
                    if(cur > max) {
                        max = cur;
                    }
                }
                if (max == 0) {
                    return null;
                }
                return max;
            }
        });

        DataFrame age = sqlContext.createDataFrame(age2.map(new Function<Tuple2<String, Integer>, Age>() {
            @Override
            public Age call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                Age result = new Age();
                result.setIdcode(stringIntegerTuple2._1().trim());
                result.setAge(stringIntegerTuple2._2());
                return result;
            }
        }),Age.class);

        age.registerTempTable("age");














        JavaPairRDD<String,Iterable<String>> advan1 = alldata.select("idcode","advan").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        return new Tuple2(row.getString(0).trim(),row.getString(1));
                    }
                }
        ).groupByKey();

        JavaPairRDD<String,String> advan2 = advan1.mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> istr) throws Exception {
                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                Iterator<String> iter = istr.iterator();
                while(iter.hasNext())
                {
                    String cur = iter.next();
                    if(cur == null) {
                        continue;
                    }
                    if(str.indexOf(cur) < 0)
                    {
                        str.add(cur);
                        num.add(1);
                    }
                    else
                    {
                        int indx = str.indexOf(cur);
                        num.set(indx,num.get(indx)+1);
                    }
                }
                if(str.size() == 0) {
                    return null;
                }
                int max = 0;
                int res_indx =0;
                for(int i=0;i<num.size();i++)
                {
                    if(max < num.get(i))
                    {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return str.get(res_indx);
            }
        });

        DataFrame advan = sqlContext.createDataFrame(advan2.map(new Function<Tuple2<String, String>, Advan>() {
            @Override
            public Advan call(Tuple2<String, String> stringStringTuple2) throws Exception {
                Advan result = new Advan();
                result.setIdcode(stringStringTuple2._1().trim());
                result.setAdvan(stringStringTuple2._2());
                return result;
            }
        }),Advan.class);

        advan.registerTempTable("advan");












//        JavaPairRDD<String,Iterable<String>> count1 = alldata.select("idcode","航班号").toJavaRDD().mapToPair(
//                new PairFunction<Row, String, String>() {
//                    @Override
//                    public Tuple2<String, String> call(Row row) throws Exception {
//                        return new Tuple2(row.getString(0).trim(),row.getString(1).trim());
//                    }
//                }
//        ).groupByKey();
//
//
//        JavaPairRDD<String,Integer> count2 = count1.mapValues(new Function<Iterable<String>, Integer>() {
//            @Override
//            public Integer call(Iterable<String> istr) throws Exception {
//                Iterator<String> iter = istr.iterator();
//                Integer result = 0;
//                while (iter.hasNext())
//                    result++;
//                return result;
//            }
//        });
//
//        DataFrame count = sqlContext.createDataFrame(count2.map(new Function<Tuple2<String, Integer>, Count>() {
//            @Override
//            public Count call(Tuple2<String, Integer> tsi) throws Exception {
//                Count result = new Count();
//                result.setIdcode(tsi._1());
//                result.setCount(tsi._2());
//                return result;
//            }
//        }),Count.class);
//



        DataFrame count = sqlContext.sql("SELECT idcode,count(idcode) AS count FROM alldata GROUP BY idcode");
        count.registerTempTable("count");





































        JavaPairRDD<String,Iterable<String>> consum1 = alldata.select("idcode","OD","航班日期","bucket","折扣").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String str2;
                        if (row.get(3)==null) {
                            str2 = "miss";
                        } else {
                            str2 = row.getString(3).trim();
                        }

                        String key = row.getString(1).trim()+","+row.getString(2).trim()+","+str2;

                        String str1;
                        if (row.get(4)==null) {
                            str1 = "miss";
                        } else {
                            str1 = row.get(4).toString().trim();
                        }

                        String value = row.getString(0).trim()+","+str1;
                        return new Tuple2(key,value);
                    }
                }
        ).groupByKey();


        JavaPairRDD<String,String> consum2 = consum1.flatMapValues(new Function<Iterable<String>, Iterable<String>>() {
            @Override
            public Iterable<String> call(Iterable<String> istr) throws Exception {
                Iterator<String> iter = istr.iterator();
                ArrayList<String> idcode = new ArrayList<String>();
                ArrayList<Double> discount = new ArrayList<Double>();
                while (iter.hasNext())
                {
                    String[] parts = iter.next().split(",");
                    idcode.add(parts[0].trim());
                    if (parts[1].equals("miss")) {
                        discount.add(null);
                    } else {
                        discount.add(Double.parseDouble(parts[1].trim()));
                    }
                }
                double sum = 0;
                int count = 0;
                for (int i=0;i<discount.size();i++) {
                    if (discount.get(i) != null) {
                        sum = sum + discount.get(i);
                        count = count + 1;
                    }
                }
                double avg = sum/count;

                ArrayList<String> result = new ArrayList<String>();
                for (int i=0;i<discount.size();i++)
                {
                    if (discount.get(i)==null)
                    {
                        result.add(idcode.get(i).trim()+","+"miss");
                        continue;
                    }
                    if (discount.get(i)<=avg) {
                        result.add(idcode.get(i).trim() + "," + "0");
                    } else {
                        result.add(idcode.get(i).trim() + "," + "1");
                    }
                }
                return result;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                String[] parts = stringStringTuple2._2().split(",");
                return new Tuple2(parts[0].trim(),parts[1].trim());
            }
        }).groupByKey().mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {
                int sum = 0;
                int count = 0;
                Iterator<String> iter = strings.iterator();
                String cur;
                while (iter.hasNext())
                {
                    cur = iter.next();
                    if (cur.equals("1")) {
                        sum = sum + 1;
                    }
                    if (!cur.equals("miss")) {
                        count = count + 1;
                    }
                }
                if (count==0) {
                    return "none";
                }
                float perc = (float)sum/count;
                if (perc<0.4) {
                    return "sens";
                } else if (perc>=0.4 && perc<0.7) {
                    return "norm";
                } else {
                    return "nosens";
                }
            }
        });

        DataFrame consum = sqlContext.createDataFrame(consum2.map(new Function<Tuple2<String, String>, Consum>() {
            @Override
            public Consum call(Tuple2<String, String> tss) throws Exception {
                Consum result = new Consum();
                result.setIdcode(tss._1().trim());
                result.setConsum(tss._2());
                return result;
            }
        }),Consum.class);

        consum.registerTempTable("consum");
























        JavaPairRDD<String,String> partner1 = alldata.select("idcode","出票时刻","航班日期","resid","航班号","idb").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String key = row.get(1).toString().trim()+","+row.getString(2).trim()+","+row.getString(4).trim();
                        String str1;
                        if (row.getString(3) == null) {
                            str1 = "miss";
                        } else {
                            str1 = row.getString(3).trim();
                        }
                        String str2;
                        if (row.getString(5) == null) {
                            str2 = "miss";
                        } else {
                            str2 = row.getString(5).trim();
                        }

                        String value = row.getString(0).trim()+","+str1+","+str2;
                        return new Tuple2(key,value);
                    }
                }
        ).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s+","+s2;
            }
        });



        JavaPairRDD<String, String> partner2 = partner1.flatMapValues(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                String[] parts = s.split(",");


                ArrayList<String> both_id = new ArrayList<String>();
                ArrayList<String> resid_id = new ArrayList<String>();
                ArrayList<String> idb_id = new ArrayList<String>();

                ArrayList<String> resid = new ArrayList<String>();
                ArrayList<String> idb = new ArrayList<String>();
                ArrayList<String> idcode = new ArrayList<String>();

                ArrayList<String> uresid = new ArrayList<String>();
                ArrayList<String> uidb = new ArrayList<String>();
                ArrayList<String> uidcode = new ArrayList<String>();

                for (int i = 0; i < parts.length; i = i + 3) {
                    idcode.add(parts[i]);
                    resid.add(parts[i + 1]);
                    idb.add(parts[i + 2]);
                }

                for (int i = 0; i < resid.size(); i++) {
                    if (!uresid.contains(resid.get(i))) {
                        uresid.add(resid.get(i));
                    }
                }

                for (int i = 0; i < idb.size(); i++) {
                    if (!uidb.contains(idb.get(i))) {
                        uidb.add(idb.get(i));
                    }
                }

                for (int i = 0; i < idcode.size(); i++) {
                    if (!uidcode.contains(idcode.get(i))) {
                        uidcode.add(idcode.get(i));
                    }
                }

                ArrayList<String> temp = new ArrayList<String>();
                int j = 0;
                int k = 0;


                for (int i = 0; i < uresid.size(); i++) {
                    if (uresid.get(i).equals("miss")) {
                        continue;
                    }
                    for (j = 0; j < resid.size(); j++) {
                        if (resid.get(j).equals(uresid.get(i))) {
                            temp.add(idcode.get(j));
                        }
                    }
                    if (temp.size() > 1) {
                        for (k = 0; k < temp.size(); k++) {
                            if (!resid_id.contains(temp.get(k))) {
                                resid_id.add(temp.get(k));
                            }
                        }
                    }
                    temp.clear();
                }

                temp.clear();

                for (int i = 0; i < uidb.size(); i++) {
                    if (uidb.get(i).equals("miss")) {
                        continue;
                    }
                    for (j = 0; j < idb.size(); j++) {
                        if (idb.get(j).equals(uidb.get(i))) {
                            temp.add(idcode.get(j));
                        }
                    }
                    if (temp.size() > 1) {
                        for (k = 0; k < temp.size(); k++) {
                            if (!idb_id.contains(temp.get(k))) {
                                idb_id.add(temp.get(k));
                            }
                        }
                    }
                    temp.clear();
                }

                for (int i = 0; i < resid_id.size(); i++) {
                    if (idb_id.contains(resid_id.get(i))) {
                        both_id.add(resid_id.get(i));
                    }
                }


                for (int i = 0; i < both_id.size(); i++) {
                    resid_id.remove(both_id.get(i));
                    idb_id.remove(both_id.get(i));
                    uidcode.remove(both_id.get(i));
                }

                for (int i = 0; i < resid_id.size(); i++) {
                    uidcode.remove(resid_id.get(i));
                }

                for (int i = 0; i < idb_id.size(); i++) {
                    uidcode.remove(idb_id.get(i));
                }

                ArrayList<String> result = new ArrayList<String>();
                for (int i = 0; i < both_id.size(); i++) {
                    result.add(both_id.get(i) + "," + "both");
                }
                for (int i = 0; i < resid_id.size(); i++) {
                    result.add(resid_id.get(i) + "," + "resid");
                }
                for (int i = 0; i < idb_id.size(); i++) {
                    result.add(idb_id.get(i) + "," + "idb");
                }
                for (int i = 0; i < uidcode.size(); i++) {
                    result.add(uidcode.get(i) + "," + "single");
                }
                return result;
            }
        });
        JavaPairRDD<String, String> partner3 = partner2.mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> stringUTuple2) throws Exception {
                String[] parts = stringUTuple2._2().split(",");
                return new Tuple2(parts[0].trim(), parts[1].trim());
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "," + s2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, String> k2IterableTuple2) throws Exception {

                String key = k2IterableTuple2._1();

                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                String[] parts = k2IterableTuple2._2().split(",");
                for (int i=0;i<parts.length;i++)
                {
                    if(!str.contains(parts[i])) {
                        str.add(parts[i]);
                        num.add(1);
                    }
                    else
                    {
                        num.set(str.indexOf(parts[i]),num.get(str.indexOf(parts[i]))+1);
                    }

                }
                Integer max = 0;
                int res_indx = -1;
                for(int i=0;i<num.size();i++)
                {
                    if(num.get(i)>max)
                    {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return new Tuple2(key,str.get(res_indx));
            }
        });



        DataFrame partner = sqlContext.createDataFrame(partner3.map(new Function<Tuple2<String, String>, Partner>() {
            @Override
            public Partner call(Tuple2<String, String> tss) throws Exception {
                Partner result = new Partner();
                result.setIdcode(tss._1().trim());
                result.setPartner(tss._2());
                return result;
            }
        }),Partner.class);

        partner.registerTempTable("partner");












        JavaPairRDD<String,String> idb1 = sqlContext.sql("SELECT idcode,idb FROM alldata").toJavaRDD().mapToPair(new PairFunction<Row, String, String>() {
            @Override
            public Tuple2<String, String> call(Row row) throws Exception {
                String key = row.getString(0).trim();
                String value;
                if (row.getString(1)==null) {
                    value = "miss";
                } else {
                    value = row.getString(1).trim();
                }
                return new Tuple2(key,value);
            }
        }).groupByKey().mapValues(new Function<Iterable<String>, String>() {
            @Override
            public String call(Iterable<String> strings) throws Exception {
                ArrayList<String> str = new ArrayList<String>();
                ArrayList<Integer> num = new ArrayList<Integer>();
                Iterator<String> iter = strings.iterator();
                String cur;
                while (iter.hasNext())
                {
                    cur = iter.next().trim();
                    if (cur.equals("miss")) {
                        continue;
                    }
                    if (!str.contains(cur))
                    {
                        str.add(cur);
                        num.add(1);
                    }
                    else {
                        num.set(str.indexOf(cur), num.get(str.indexOf(cur)) + 1);
                    }
                }
                if (str.size()<1) {
                    return null;
                }
                int max = 0;
                int res_indx = -1;
                for(int i=0;i<str.size();i++) {
                    if (num.get(i) > max) {
                        max = num.get(i);
                        res_indx = i;
                    }
                }
                return str.get(res_indx);
            }
        });

        DataFrame idb = sqlContext.createDataFrame(idb1.map(new Function<Tuple2<String,String>,Idb>() {
            @Override
            public Idb call(Tuple2<String,String> tss) throws Exception {
                Idb result = new Idb();
                String str;

                result.setIdcode(tss._1().trim());
                result.setIdb(tss._2());
                return result;
            }
        }),Idb.class);

        idb.registerTempTable("idb");




















        DataFrame user1 = sqlContext.sql("SELECT ticketChannel.idcode AS idcode," +
                "ticketChannel,paymentMode,vip,groupMark,age,advan,count,resid,consum,partner,idb " +
                "FROM ticketChannel,paymentMode,vip,groupMark,age,advan,count,resid,consum,partner,idb " +
                "WHERE ticketChannel.idcode=paymentMode.idcode " +
                "AND paymentMode.idcode=vip.idcode " +
                "AND vip.idcode=groupMark.idcode " +
                "AND groupMark.idcode=age.idcode " +
                "AND age.idcode=advan.idcode " +
                "AND advan.idcode=count.idcode " +
                "AND count.idcode=resid.idcode " +
                "AND resid.idcode=consum.idcode " +
                "AND consum.idcode=partner.idcode " +
                "AND partner.idcode=idb.idcode").cache();

        user1.registerTempTable("user1");





        JavaPairRDD<String,String> freque1 = user1.select("idcode","vip","count").toJavaRDD().mapToPair(
                new PairFunction<Row, String, String>() {
                    @Override
                    public Tuple2<String, String> call(Row row) throws Exception {
                        String vip = row.getString(1).trim();
                        Long count = row.getLong(2);
                        String value;
                        if(vip.equals("1") || count>=6) {
                            value = "1";
                        } else {
                            value = "0";
                        }

                        return new Tuple2(row.getString(0).trim(),value);
                    }
                }
        );


        DataFrame freque = sqlContext.createDataFrame(freque1.map(new Function<Tuple2<String, String>, Freque>() {
            @Override
            public Freque call(Tuple2<String, String> tss) throws Exception {
                Freque result = new Freque();
                result.setIdcode(tss._1());
                result.setFreque(tss._2());
                return result;
            }
        }),Freque.class);

        freque.registerTempTable("freque");










        DataFrame user = sqlContext.sql("SELECT user1.*,freque FROM user1,freque WHERE user1.idcode=freque.idcode");



        user.write().parquet("hdfs://108.108.108.15/USER/root/csair/user_new.parquet");













//        System.out.println("ticketChannel:"+ticketChannel.count()+"\n"
//                            +"paymentMode:"+paymentMode.count()+"\n"
//                            +"age:"+age.count()+"\n"
//                            +"vip:"+vip.count()+"\n"
//                            +"groupMark:"+groupMark.count()+"\n"
//                            +"advan:"+advan.count()+"\n"
//                            +"count:"+count.count()+"\n"
//                            +"resid:"+resid.count()+"\n"
//                            +"consum:"+consum.count()+"\n"
//                            +"partner:"+partner.count());


        jsc.stop();
    }
}
