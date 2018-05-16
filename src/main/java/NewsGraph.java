import com.alibaba.fastjson.JSONObject;
import dao.impl.Neo4jDao;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import scala.Int;
import scala.Tuple2;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

import static org.apache.spark.sql.functions.col;

/**
 * @author lsx
 * @date 2018/3/27
 */
public class NewsGraph {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://rm-uf65d386n0372w0tsmo.mysql.rds.aliyuncs.com:3306/propaganda_culture_cloud_data_supply?useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    static final String USER = "gykj_mysql";
    static final String PASS = "8y%#oSHbodJRlVsCYgvB";

    static final String VERTEX_TABLE = "dataman_knowtoop1_piont_drop";
    static final String EDGE_TABLE = "dataman_kowntoop1_side";
    static final String TOPICINFO_TABLE = "dataman_knowtoop1_topicinfo";
    static final String ARTICLE_TABLE = "dataman_knowtoop1_articledata";
    static final String ENTRY_TABLE = "dataman_knowtoop1_entries";


    /**
     * 对list中的元素计数
     *
     * @param list
     * @return
     */
    private static Map<String, Integer> counterMap(List<String> list) {
        Map<String, Integer> result = new HashMap<>();
        for (String str : list) {
            if (result.containsKey(str)) {
                result.put(str, result.get(str) + 1);
            } else {
                result.put(str, 1);
            }
        }
        return result;
    }

    private static Map<String, Integer> mergeCounterMap(Map<String, Integer> map1, Map<String, Integer> map2) {
        for (Map.Entry<String, Integer> entry : map2.entrySet()) {
            if (map1.containsKey(entry.getKey())) {
                map1.put(entry.getKey(), map1.get(entry.getKey()) + 1);
            } else {
                map1.put(entry.getKey(), 1);
            }
        }
        return map1;
    }

    private static void addCounterMap(Map<String, Integer> map, List<String> list) {
        for (String str : list) {
            if (map.containsKey(str)) {
                map.put(str, map.get(str) + 1);
            } else {
                map.put(str, 1);
            }
        }
    }

    private static void addCounterMap(Map<String, Integer> map, String str) {
        if (map.containsKey(str)) {
            map.put(str, map.get(str) + 1);
        } else {
            map.put(str, 1);
        }
    }

    /**
     * 插入图谱节点表
     *
     * @param conn
     * @param tableName
     * @param value
     */
    public static void insertVertex(Connection conn, String tableName, JSONObject value) {
        try {
            PreparedStatement psql = conn.prepareStatement("INSERT IGNORE INTO " + tableName + "(id,name,value,type,summary)" + "values(?,?,?,?,?)");
            psql.setString(1, value.getString("id"));
            psql.setString(2, value.getString("name"));
            psql.setInt(3, value.getInteger("value"));
            psql.setInt(4, value.getInteger("type"));
            psql.setString(5, value.getString("summary"));
            psql.executeUpdate();
            psql.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("数据插入 " + tableName + " 成功！" + "\n");
        }
    }

    /**
     * 插入图谱边表
     *
     * @param conn
     * @param tableName
     * @param value
     */
    public static void insertEdge(Connection conn, String tableName, JSONObject value) {
        try {
            PreparedStatement psql = conn.prepareStatement("INSERT IGNORE INTO " + tableName + "(startId,endId)" + "values(?,?)");
            psql.setString(1, value.getString("startId"));
            psql.setString(2, value.getString("endId"));
            psql.executeUpdate();
            psql.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("数据插入 " + tableName + " 成功！" + "\n");
        }
    }

    /**
     * 插入文章信息表
     *
     * @param conn
     * @param tableName
     * @param value
     */
    public static void insertArticle(Connection conn, String tableName, JSONObject value) {
        try {
            PreparedStatement psql = conn.prepareStatement("INSERT IGNORE INTO " + tableName + "(id,title,source,publishTime,weight,articleId)" + "values(?,?,?,?,?,?)");
            psql.setString(1, value.getString("topicId"));
            psql.setString(2, value.getString("title"));
            psql.setString(3, value.getString("mediaName"));
            psql.setString(4, value.getString("publishTime"));
            psql.setInt(5, 1);
            psql.setString(6, value.getString("docId"));
            psql.executeUpdate();
            psql.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("数据插入 " + tableName + " 成功！" + "\n");
        }
    }

    /**
     * 插入词条信息表
     *
     * @param conn
     * @param tableName
     * @param value
     */
    public static void insertEntry(Connection conn, String tableName, JSONObject value) {
        try {
            PreparedStatement psql = conn.prepareStatement("INSERT IGNORE INTO " + tableName + "(id,title,content,weight,entriesId)" + "values(?,?,?,?,?)");
            psql.setString(1, value.getString("topicId"));
            psql.setString(2, value.getString("keywords"));
            psql.setString(3, null);
            psql.setInt(4, 1);
            psql.setString(5, value.getString("keywords"));
            psql.executeUpdate();
            psql.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("数据插入 " + tableName + " 成功！" + "\n");
        }
    }

    public static void insertTopicInfo(Connection conn, String tableName, JSONObject value) {
        try {
            PreparedStatement psql = conn.prepareStatement("INSERT IGNORE INTO " + tableName + "(id,topicId,name,weight,summary,type)" + "values(?,?,?,?,?,?)");
            psql.setString(1, value.getString("id"));
            psql.setString(2, value.getString("topicId"));
            psql.setString(3, value.getString("name"));
            psql.setInt(4, value.getInteger("value"));
            psql.setString(5, value.getString("summary"));
            psql.setInt(6, value.getInteger("type"));
            psql.executeUpdate();
            psql.close();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("数据插入 " + tableName + " 成功！" + "\n");
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.setProperty("hadoop.home.dir", "D:\\winutils");

        SparkConf sc = new SparkConf();
        sc.setMaster("local[*]").setAppName("text");

        JavaSparkContext jsc = new JavaSparkContext(sc);
        SQLContext sqlContext = new SQLContext(jsc);

        List<String> leaderList = jsc.textFile("D:\\分析项目\\错别字\\3\\leaderName.txt")
                .collect();
        final Broadcast<List<String>> leaderBroadcast = jsc.broadcast(leaderList);

        Map<String, String> placeMap = jsc.textFile("D:\\Downloads\\gzname.txt")
                .mapToPair(new PairFunction<String, String, String>() {
                    @Override
                    public Tuple2<String, String> call(String s) throws Exception {
                        String key = s;
                        if (s.endsWith("市") || s.endsWith("县") || s.endsWith("区") || s.endsWith("省")) {
                            key = key.substring(0, key.length() - 1);
                        }
                        return new Tuple2<>(key, s);
                    }
                })
                .collectAsMap();
        final Broadcast<Map<String, String>> placeBroadcast = jsc.broadcast(placeMap);


        sqlContext.udf().register("getPerson", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                List<String> result = new ArrayList<>();
                Set<String> resultSet = new HashSet<>();
                if (s == null || s.length() < 1) {
                    return result;
                }

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
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));

        sqlContext.udf().register("getPlace", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                List<String> result = new ArrayList<>();
                Set<String> resultSet = new HashSet<>();
                if (s == null || s.length() < 1) {
                    return result;
                }

                String[] terms = s.split(" ");
                for (String term : terms) {
                    String[] parts = term.split("/");
                    if (parts.length < 2) {
                        continue;
                    }
                    if ("ns".equals(parts[1])) {
                        String place = parts[0];
                        if (parts[0].endsWith("市") || parts[0].endsWith("县") || parts[0].endsWith("区") || parts[0].endsWith("省")) {
                            place = place.substring(0, place.length() - 1);
                        }
                        if (placeBroadcast.value().containsKey(place)) {
                            resultSet.add(placeBroadcast.value().get(place));
                        }
                    }
                }
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));

        sqlContext.udf().register("getOrganization", new UDF1<String, List<String>>() {
            @Override
            public List<String> call(String s) throws Exception {
                List<String> result = new ArrayList<>();
                Set<String> resultSet = new HashSet<>();
                if (s == null || s.length() < 1) {
                    return result;
                }

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
                result.addAll(resultSet);
                return result;
            }
        }, DataTypes.createArrayType(DataTypes.StringType));


        // 热点话题
        DataFrame keyphrase = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/topic/keyphrase_filter.parquet")
                .cache();
        keyphrase.registerTempTable("keyphrase");
        // 聚类结果
        DataFrame npData = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/topic/curDataTopic.parquet")
                .cache();
        npData.registerTempTable("npData");
        // 分词结果
        DataFrame pData = sqlContext.read()
                .parquet("hdfs://90.90.90.5:8020/user/ddp/AnalysisProject/topic/curData.parquet")
                .cache();
        pData.registerTempTable("pData");

        DataFrame joined = sqlContext.sql("SELECT pData.url," +
                "npData.topicId," +
                "pData.removedContent_crawler," +
                "pData.media," +
                "pData.title," +
                "pData.publishTime," +
                "pData.keywords," +
                "keyphrase.phrase " +
                "FROM pData,npData,keyphrase " +
                "WHERE pData.url=npData.url " +
                "AND npData.topicId=keyphrase.topicId");

        joined = joined.withColumn("textPerson",
                functions.callUDF("getPerson", col("removedContent_crawler")));
        joined = joined.withColumn("textPlace",
                functions.callUDF("getPlace", col("removedContent_crawler")));
        joined = joined.withColumn("textOrganization",
                functions.callUDF("getOrganization", col("removedContent_crawler")));
        joined = joined.filter("keywords IS NOT NULL");

        JavaPairRDD<String, JSONObject> reducedRDD = joined.select("phrase",
                "textPerson",
                "textPlace",
                "textOrganization",
                "media",
                "topicId",
                "url",
                "title",
                "publishTime",
                "keywords")
                .toJavaRDD()
                .mapToPair(new PairFunction<Row, String, JSONObject>() {
                    @Override
                    public Tuple2<String, JSONObject> call(Row row) throws Exception {
                        String phrase = row.getString(0);
                        List<String> textPerson = row.getList(1);
                        List<String> textPlace = row.getList(2);
                        List<String> textOrganization = row.getList(3);
                        String mediaName = row.getString(4);
                        String topicId = row.getString(5);
                        String docId = row.getString(6);
                        String title = row.getString(7);
                        String publishTime = row.getString(8);
                        List<String> keywords = new LinkedList<>();
                        String[] keywordsArray = row.getString(9).split(" ");
                        Collections.addAll(keywords, keywordsArray);

                        JSONObject newsJson = new JSONObject();
                        newsJson.put("docId", docId);
                        newsJson.put("title", title);
                        newsJson.put("publishTime", publishTime);
                        newsJson.put("mediaName", mediaName);
                        newsJson.put("topicId", topicId);

                        JSONObject result = new JSONObject();
                        result.put("person", textPerson);
                        result.put("place", textPlace);
                        result.put("organization", textOrganization);
                        result.put("mediaName", mediaName);
                        result.put("news", newsJson);
                        result.put("keywords", keywords);

                        return new Tuple2<>(phrase + ";" + topicId, result);
                    }
                })
                .groupByKey()
                .mapValues(new Function<Iterable<JSONObject>, JSONObject>() {
                    @Override
                    public JSONObject call(Iterable<JSONObject> jsonObjects) throws Exception {
                        Map<String, Integer> person = new HashMap<>();
                        Map<String, Integer> place = new HashMap<>();
                        Map<String, Integer> organization = new HashMap<>();
                        Map<String, Integer> mediaName = new HashMap<>();
                        List<JSONObject> newsList = new LinkedList<>();
                        Set<String> keywords = new HashSet<>();

                        for (JSONObject jsonObject : jsonObjects) {
                            List<String> curPerson = jsonObject.getObject("person", List.class);
                            List<String> curPlace = jsonObject.getObject("place", List.class);
                            List<String> curOrganization = jsonObject.getObject("organization", List.class);
                            String curMediaName = jsonObject.getString("mediaName");
                            JSONObject curNews = jsonObject.getJSONObject("news");
                            List<String> curKeywords = jsonObject.getObject("keywords", LinkedList.class);

                            addCounterMap(person, curPerson);
                            addCounterMap(place, curPlace);
                            addCounterMap(organization, curOrganization);
                            if (curMediaName.trim().length() > 0) {
                                addCounterMap(mediaName, curMediaName);
                            }
                            newsList.add(curNews);
                            keywords.addAll(curKeywords);
                        }

                        JSONObject result = new JSONObject();
                        result.put("person", person);
                        result.put("place", place);
                        result.put("organization", organization);
                        result.put("mediaName", mediaName);
                        result.put("news", newsList);
                        result.put("keywords", keywords);

                        return result;
                    }
                })
                .cache();

        // 插入到Neo4j
        reducedRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, JSONObject>>>() {
            @Override
            public void call(Iterator<Tuple2<String, JSONObject>> tuple2Iterator) throws Exception {
                Neo4jDao neo4jDao = new Neo4jDao();

                for (Iterator<Tuple2<String, JSONObject>> it = tuple2Iterator; it.hasNext(); ) {
                    Tuple2<String, JSONObject> tuple2 = it.next();

                    String[] parts = tuple2._1().split(";");
                    String phrase = parts[0];
                    String topicId = parts[1];
                    Map<String, Integer> person = tuple2._2().getObject("person", HashMap.class);
                    Map<String, Integer> place = tuple2._2().getObject("place", HashMap.class);
                    Map<String, Integer> organization = tuple2._2().getObject("organization", HashMap.class);
                    Map<String, Integer> mediaName = tuple2._2().getObject("mediaName", HashMap.class);

                    for (Map.Entry<String, Integer> curPerson : person.entrySet()) {
                        neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                "MERGE (n:Person{name:'" + curPerson.getKey() + "'}) " +
                                "MERGE (m)-[r:Person]-(n)");
                    }

                    for (Map.Entry<String, Integer> curPlace : place.entrySet()) {
                        neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                "MERGE (n:Place{name:'" + curPlace.getKey() + "'}) " +
                                "MERGE (m)-[r:Place]-(n)");
                    }

                    for (Map.Entry<String, Integer> curOrganization : organization.entrySet()) {
                        neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                "MERGE (n:Organization{name:'" + curOrganization.getKey() + "'}) " +
                                "MERGE (m)-[r:Organization]-(n)");
                    }

                    for (Map.Entry<String, Integer> curMedia : mediaName.entrySet()) {
                        neo4jDao.createRelation("MERGE (m:Topic{name:'" + phrase + "'}) " +
                                "MERGE (n:Media{name:'" + curMedia.getKey() + "'}) " +
                                "MERGE (m)-[r:Media]-(n)");
                    }
                }
                neo4jDao.close();
            }
        });

        // 插入到Mysql
//        reducedRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, JSONObject>>>() {
//            @Override
//            public void call(Iterator<Tuple2<String, JSONObject>> tuple2Iterator) throws Exception {
//                Class.forName(JDBC_DRIVER);
//                Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
//
//                for (Iterator<Tuple2<String, JSONObject>> it = tuple2Iterator; it.hasNext(); ) {
//                    Tuple2<String, JSONObject> tuple2 = it.next();
//
//                    String[] parts = tuple2._1().split(";");
//                    String phrase = parts[0];
//                    String topicId = parts[1];
//                    Map<String, Integer> person = tuple2._2().getObject("person", HashMap.class);
//                    Map<String, Integer> place = tuple2._2().getObject("place", HashMap.class);
//                    Map<String, Integer> organization = tuple2._2().getObject("organization", HashMap.class);
//                    Map<String, Integer> mediaName = tuple2._2().getObject("mediaName", HashMap.class);
//                    List<JSONObject> news = tuple2._2().getObject("news", LinkedList.class);
//                    Set<String> keywords = tuple2._2().getObject("keywords", HashSet.class);
//
//                    JSONObject topicVertex = new JSONObject();
//                    topicVertex.put("id", topicId);
//                    topicVertex.put("name", phrase);
//                    topicVertex.put("value", 2);
//                    topicVertex.put("type", 1);
//                    topicVertex.put("summary", keywords.toString());
//
//                    insertVertex(connection, VERTEX_TABLE, topicVertex);
//
//                    for (Map.Entry<String, Integer> curPerson : person.entrySet()) {
//                        JSONObject vertex = new JSONObject();
//                        vertex.put("id", curPerson.getKey());
//                        vertex.put("name", curPerson.getKey());
//                        vertex.put("value", curPerson.getValue());
//                        vertex.put("type", 2);
//                        vertex.put("summary", curPerson.getKey());
//                        vertex.put("topicId", topicId);
//
//                        insertVertex(connection, VERTEX_TABLE, vertex);
//                        insertTopicInfo(connection, TOPICINFO_TABLE, vertex);
//
//                        JSONObject edge = new JSONObject();
//                        edge.put("startId", topicId);
//                        edge.put("endId", curPerson.getKey());
//
//                        insertEdge(connection, EDGE_TABLE, edge);
//                    }
//
//                    for (Map.Entry<String, Integer> curPlace : place.entrySet()) {
//                        JSONObject vertex = new JSONObject();
//                        vertex.put("id", curPlace.getKey());
//                        vertex.put("name", curPlace.getKey());
//                        vertex.put("value", curPlace.getValue());
//                        vertex.put("type", 3);
//                        vertex.put("summary", curPlace.getKey());
//                        vertex.put("topicId", topicId);
//
//                        insertVertex(connection, VERTEX_TABLE, vertex);
//                        insertTopicInfo(connection, TOPICINFO_TABLE, vertex);
//
//                        JSONObject edge = new JSONObject();
//                        edge.put("startId", topicId);
//                        edge.put("endId", curPlace.getKey());
//
//                        insertEdge(connection, EDGE_TABLE, edge);
//                    }
//
//                    for (Map.Entry<String, Integer> curOrganization : organization.entrySet()) {
//                        JSONObject vertex = new JSONObject();
//                        vertex.put("id", curOrganization.getKey());
//                        vertex.put("name", curOrganization.getKey());
//                        vertex.put("value", curOrganization.getValue());
//                        vertex.put("type", 4);
//                        vertex.put("summary", curOrganization.getKey());
//                        vertex.put("topicId", topicId);
//
//                        insertVertex(connection, VERTEX_TABLE, vertex);
//                        insertTopicInfo(connection, TOPICINFO_TABLE, vertex);
//
//                        JSONObject edge = new JSONObject();
//                        edge.put("startId", topicId);
//                        edge.put("endId", curOrganization.getKey());
//
//                        insertEdge(connection, EDGE_TABLE, edge);
//                    }
//
//                    for (Map.Entry<String, Integer> curMedia : mediaName.entrySet()) {
//                        JSONObject vertex = new JSONObject();
//                        vertex.put("id", curMedia.getKey());
//                        vertex.put("name", curMedia.getKey());
//                        vertex.put("value", curMedia.getValue());
//                        vertex.put("type", 5);
//                        vertex.put("summary", curMedia.getKey());
//                        vertex.put("topicId", topicId);
//
//                        insertVertex(connection, VERTEX_TABLE, vertex);
//                        insertTopicInfo(connection, TOPICINFO_TABLE, vertex);
//
//                        JSONObject edge = new JSONObject();
//                        edge.put("startId", topicId);
//                        edge.put("endId", curMedia.getKey());
//
//                        insertEdge(connection, EDGE_TABLE, edge);
//                    }
//
//                    for (JSONObject newsJson : news) {
//                        insertArticle(connection, ARTICLE_TABLE, newsJson);
//                    }
//
////                    for (String curKeywords : keywords) {
////                        JSONObject entry = new JSONObject();
////                        entry.put("topicId", topicId);
////                        entry.put("keywords", curKeywords);
//
////                        insertEntry(connection, ENTRY_TABLE, entry);
////                    }
//                }
//                connection.close();
//            }
//        });

        jsc.stop();
    }
}
