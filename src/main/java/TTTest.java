import com.alibaba.fastjson.JSONObject;
import com.kafka.KafkaUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.deploy.worker.Sleeper;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 *
 * @author lsx
 * @date 2018/3/27
 */
public class TTTest {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir","D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        DataFrame dataFrame = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/lsx/MRPResult/p_allData.parquet/year=2018")
                .filter("mediaName='多彩贵州网'")
                .sample(false, 0.005);

//        System.out.println(dataFrame.count());
//        dataFrame.show();

        dataFrame.select("docId", "title", "text")
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("D:\\分析项目\\错别字\\3\\test.parquet");
    }
}
