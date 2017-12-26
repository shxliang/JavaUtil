package hotwords;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author lsx
 */
public class MergeDataToParquet {
    public static void main(String[] args) {
        SparkConf sc = new SparkConf();
//        sc.setMaster("local[*]").setAppName("Test");
        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        JavaRDD<String> stringJavaRDD = jsc.textFile("hdfs://90.90.90.5:8020/user/TianyuanPan/panfago");
        JavaRDD<Row> rowJavaRDD = stringJavaRDD.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String segmentedAll = jsonObject.getString("segmentedAll");
                String keywords = jsonObject.getString("keywords");
                String textPlace = "";
                if (jsonObject.containsKey("textPlace"))
                {
                    textPlace = jsonObject.getString("textPlace");
                }
                String textPerson = "";
                if (jsonObject.containsKey("textPerson"))
                {
                    textPerson = jsonObject.getString("textPerson");
                }
                String docClass = jsonObject.getString("docClass").split(" ")[0];
                return RowFactory.create(docClass, keywords, textPerson, textPlace, segmentedAll);
            }
        });
        StructType schema = new StructType(new StructField[]{
                new StructField("docClass", DataTypes.StringType, false, Metadata.empty()),
                new StructField("keywords", DataTypes.StringType, false, Metadata.empty()),
                new StructField("textPerson", DataTypes.StringType, false, Metadata.empty()),
                new StructField("textPlace", DataTypes.StringType, false, Metadata.empty()),
                new StructField("segmentedAll", DataTypes.StringType, false, Metadata.empty())
        });
        DataFrame dataFrame = sqlContext.createDataFrame(rowJavaRDD, schema);
        dataFrame.repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet("hdfs://90.90.90.5:8020/user/lsx/17hotwords/mrp_data.parquet");

        jsc.stop();
    }
}
