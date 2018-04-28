import com.alibaba.fastjson.JSONObject;
import dao.impl.Neo4jDao;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Tuple2;

import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * @author lsx
 * @date 2018/3/27
 */
public class NewsGraph {
    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> leaderList = jsc.textFile("D:\\分析项目\\错别字\\3\\leaderName.txt").collect();
        final Broadcast<List<String>> leaderBroadcast = jsc.broadcast(leaderList);


        sqlContext.udf().register("getPerson", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                Set<String> resultSet = new HashSet<>();
                String[] terms = s.split(" ");
                for (String term : terms) {
                    String[] parts = term.split("/");
                    if (parts.length < 2) {
                        continue;
                    }
                    if ("nr".equals(parts[1])) {
                        if (leaderBroadcast.value().contains(parts[0])) {
                            resultSet.add(parts[0]);
                        }
                    }
                }
                List<String> result = new ArrayList<>();
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));

        sqlContext.udf().register("getPlace", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                Set<String> resultSet = new HashSet<>();
                String[] terms = s.split(" ");
                for (String term : terms) {
                    String[] parts = term.split("/");
                    if (parts.length < 2) {
                        continue;
                    }
                    if ("ns".equals(parts[1])) {
                        String place = parts[0];
                        if (parts[0].endsWith("市") || parts[0].endsWith("县") || parts[0].endsWith("省")) {
                            place = place.substring(0, place.length() - 1);
                        }
                        if (place.length() <= 1) {
                            continue;
                        }
                        resultSet.add(place);
                    }
                }
                List<String> result = new ArrayList<>();
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));

        sqlContext.udf().register("getOrganization", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                Set<String> resultSet = new HashSet<>();
                String[] terms = s.split(" ");
                for (String term : terms) {
                    String[] parts = term.split("/");
                    if (parts.length < 2) {
                        continue;
                    }
                    if ("nt".equals(parts[1])) {
                        resultSet.add(parts[0]);
                    }
                }
                List<String> result = new ArrayList<>();
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));


        DataFrame keyphrase = sqlContext.read()
                .parquet("D:/Downloads/keyphrase.parquet")
                .cache();
        keyphrase.registerTempTable("keyphrase");
        DataFrame npData = sqlContext.read()
                .parquet("D:/Downloads/np_data_03-04.parquet")
                .cache();
        npData.registerTempTable("npData");
        final DataFrame pData = sqlContext.read()
                .parquet("D:/Downloads/p_data_04.parquet")
                .cache();
        pData.registerTempTable("pData");

        DataFrame joined = sqlContext.sql("SELECT pData.docId," +
                "npData.topicId," +
                "pData.removedText," +
                "pData.mediaName," +
                "keyphrase.phrase " +
                "FROM pData,npData,keyphrase " +
                "WHERE pData.docId=npData.docId " +
                "AND npData.topicId=keyphrase.topicId");

        joined = joined.withColumn("textPerson",
                functions.callUDF("getPerson", col("removedText")));
        joined = joined.withColumn("textPlace",
                functions.callUDF("getPlace", col("removedText")));
        joined = joined.withColumn("textOrganization",
                functions.callUDF("getOrganization", col("removedText")));

        joined.select("phrase", "textPerson", "textPlace", "textOrganization", "mediaName")
                .toJavaRDD()
                .mapToPair(new PairFunction<Row, String, JSONObject>() {
                    @Override
                    public Tuple2<String, JSONObject> call(Row row) throws Exception {
                        String phrase = row.getString(0);
                        List<String> textPerson = row.getList(1);
                        List<String> textPlace = row.getList(2);
                        List<String> textOrganization = row.getList(3);
                        String mediaName = row.getString(4);

                        JSONObject json = new JSONObject();
                        json.put("person", textPerson);
                        json.put("place", textPlace);
                        json.put("organization", textOrganization);
                        json.put("mediaName", mediaName);

                        return new Tuple2<>(phrase, json);
                    }
                })
                .groupByKey()
                .mapValues(new Function<Iterable<JSONObject>, JSONObject>() {
                    @Override
                    public JSONObject call(Iterable<JSONObject> jsonObjects) throws Exception {
                        Set<String> person = new HashSet<>();
                        Set<String> place = new HashSet<>();
                        Set<String> organization = new HashSet<>();
                        Set<String> mediaName = new HashSet<>();

                        for (JSONObject jsonObject : jsonObjects) {
                            List<String> curPerson = (List<String>) jsonObject.get("person");
                            List<String> curPlace = (List<String>) jsonObject.get("place");
                            List<String> curOrganization = (List<String>) jsonObject.get("organization");
                            String curMediaName = jsonObject.getString("mediaName");

                            person.addAll(curPerson);
                            place.addAll(curPlace);
                            organization.addAll(curOrganization);
                            if (curMediaName.trim().length() > 0) {
                                mediaName.add(curMediaName);
                            }
                        }

                        JSONObject result = new JSONObject();
                        result.put("person", person);
                        result.put("place", place);
                        result.put("organization", organization);
                        result.put("mediaName", mediaName);
                        return result;
                    }
                })
                .foreachPartition(new VoidFunction<Iterator<Tuple2<String, JSONObject>>>() {
                    @Override
                    public void call(Iterator<Tuple2<String, JSONObject>> tuple2Iterator) throws Exception {
                        Neo4jDao neo4jDao = new Neo4jDao();

                        for (Iterator<Tuple2<String, JSONObject>> it = tuple2Iterator; it.hasNext(); ) {
                            Tuple2<String, JSONObject> tuple2 = it.next();

                            String phrase = tuple2._1();
                            Set<String> person = (Set<String>) tuple2._2().get("person");
                            Set<String> place = (Set<String>) tuple2._2().get("place");
                            Set<String> organization = (Set<String>) tuple2._2().get("organization");
                            Set<String> mediaName = (Set<String>) tuple2._2().get("mediaName");

                            for (String curPerson : person) {
                                neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                        "MERGE (n:Person{name:'" + curPerson + "'}) " +
                                        "MERGE (m)-[r:Person]-(n)");
                            }

                            for (String curPlace : place) {
                                neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                        "MERGE (n:Place{name:'" + curPlace + "'}) " +
                                        "MERGE (m)-[r:Place]-(n)");
                            }

                            for (String curOrganization : organization) {
                                neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                        "MERGE (n:Organization{name:'" + curOrganization + "'}) " +
                                        "MERGE (m)-[r:Organization]-(n)");
                            }

                            for (String curMedia : mediaName) {
                                neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                        "MERGE (n:Media{name:'" + curMedia + "'}) " +
                                        "MERGE (m)-[r:Media]-(n)");
                            }
                        }
                        neo4jDao.close();
                    }
                });

        jsc.stop();
    }
}
