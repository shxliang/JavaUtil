import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.Seq;


import java.io.Serializable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by macan on 2017/11/7.
 */
public class MaCan implements Serializable{

    private final static String HDFSpath2016 = "hdfs://90.90.90.5:8020/ddp/year=2016";
    private final static String HDFSpath2017 = "hdfs://90.90.90.5:8020/ddp/year=2017";


    /**
     * 从指定的path 目录中加载新闻数据
     * "hdfs://90.90.90.5:8020/ddp/year=2016 

     "
     *
     * @return 其中的word列为实体集合字符串，每个实体通过空格分开。
     */
    private DataFrame loadHDFSData(SQLContext sqlContext, String path){
        DataFrame allDF = sqlContext.read()
                .parquet(path).limit(1000000);
        //过滤掉标题中的表情符号
        sqlContext.udf().register("tmp", new UDF1<String, String>() {
            @Override
            public String call(String s) throws Exception {
                String pattern = "[\ud83c\udc00-\ud83c\udfff]|[\ud83d\udc00-\ud83d\udfff]|[\u2600-\u27ff]";
                String reStr = "";
                Pattern emoji = Pattern.compile(pattern);
                Matcher emojiMatcher = emoji.matcher(s);
                s = emojiMatcher.replaceAll(reStr);
                return s;
            }
        }, DataTypes.StringType);

        allDF = allDF.select("title", "text", "title", "tags");
        allDF = allDF.withColumn("titles", functions.callUDF("tmp",functions.col("title")))
                .drop("title");
        return allDF;
    }

    private static Pattern pattern = Pattern.compile("(column,).*?(,)");

    /**
     * 过滤没有分类标签的文章
     * @param sqlContext
     * @param dataFrame
     * @return
     */
    public DataFrame filterClassification(SQLContext sqlContext, DataFrame dataFrame) {

        sqlContext.udf().register("tmp", new UDF1<Seq, String>() {
            @Override
            public String call(Seq s) throws Exception {
                String line = s.toString();
                Matcher matcher = pattern.matcher(line);
                if (matcher.find()){
                    line = matcher.group(0).replaceAll("[a-z,]", "");
                    return line;
                }
                return null;
            }
        }, DataTypes.StringType);
        dataFrame = dataFrame.withColumn("tag", functions.callUDF("tmp", functions.col("tags")))
                .drop("tags")
                .filter("tag is not NULL");
        return dataFrame;
    }

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir","D:\\winutils");
        SparkConf sc = new SparkConf().setMaster("local[*]").setAppName("LoadData");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        MaCan classification = new MaCan();
        // load data
        DataFrame data = classification.loadHDFSData(sqlContext, HDFSpath2017);
        data = classification.filterClassification(sqlContext, data);
//        data.select("tag").show(false);
        data.repartition(1).write().json("src/main/resources/classification");
    }
}