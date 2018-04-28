package dao;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.exceptions.ClientException;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Created by Macan on 2018/1/2.
 * neo4j jdbc base dao
 */
public abstract class BaseDao implements Serializable, AutoCloseable{

    public static final String URL =  "bolt://118.118.118.213:7687";
    public static final String username = "neo4j";
    public static final String password = "1";

    private final org.neo4j.driver.v1.Driver driver;
    public StatementResult rst = null;
    public Session session = null;


    public BaseDao() {
        driver = GraphDatabase.driver(URL, AuthTokens.basic(username, password), Config.build()
                .withConnectionLivenessCheckTimeout(30, TimeUnit.SECONDS).withEncryption().toConfig());
    }

    @Override
    public void close() throws Exception{
        driver.close();
    }


//    public void getConnection() {
//        driver = GraphDatabase.driver(URL, AuthTokens.basic(username, password), Config.build()
//                .withConnectionLivenessCheckTimeout(15, TimeUnit.SECONDS).toConfig());
//
//    }

//    public void close() {
//        synchronized (BaseDao.class) {
//            if (null != session) {
//                session.close();
//                session = null;
//            }
//            if (driver != null) {
//                driver.close();
//                driver = null;
//            }
//        }
//    }

    public int excuteUpdate(String sql, Object... params) {
//            getConnection();
        final String sqls = setStatement(sql, params);
        if (sqls.contains("?")){
            return -1;
        }
        try (Session session = driver.session()){
            session.writeTransaction(new TransactionWork<Integer>() {
                @Override
                public Integer execute(Transaction transaction) {
                    transaction.run(sqls);
                    return 1;
                }
            });
//            sql = setStatement(sql, params);11
//            StatementResult rst = session.run(sql);
        }
//        try {
//            session = driver.session();
//            sql = setStatement(sql, params);
//            StatementResult rst = session.run(sql);
//        }catch (org.neo4j.driver.v1.exceptions.ClientException e){
//            e.printStackTrace();
//        }

        return 1;
//
//        return 0;
    }

    public StatementResult excuteQuery(String sql, Object... params) {
        try {
            session = driver.session();
            sql = setStatement(sql, params);
            rst = session.run(sql);
        }catch (ClientException e ){
            e.printStackTrace();
        }
        return rst;
    }

    public static String setStatement(String sql, Object... params){
//        return String.format(sql, );
        for (int i = 0; i < params.length; ++i) {
            try {
                sql = sql.replaceFirst("\\?", params[i].toString());
            }catch (IllegalArgumentException e){
                e.printStackTrace();
//                System.out.println(e.);
            }
        }
        return sql;

    }

    /**
     * 将标签list转化为标签string
     * @param labels
     * @return
     */
    public static String getLabels(List<String> labels) {
        String result = "";
        for (int i = 0; i < labels.size() -1; ++i) {
            result += labels.get(i) + ":";
        }
        result += labels.get(labels.size() -1);

        return result;
    }

    /**
     * 将属性map转化为属性string
     * @param property
     * @return
     */
    public static String getProperty(Map<String, String> property) {
        String result = "";
        for (Map.Entry<String, String> entry : property.entrySet()) {
            result += entry.getKey() + ":" + "\"" + entry.getValue() + "\"" + ",";
        }
        //去掉最后一个,
        if (result.endsWith(",")) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }

    /**
     * 以and 的形式，拼接
     * @param property
     * @return
     */
    public static String getProperty2(Map<String, String> property) {
        String result = "";
        for (Map.Entry<String, String> entry : property.entrySet()) {
            result += "m." + entry.getKey() + "=" + "\"" + entry.getValue() + "\"" + " and ";
        }
        //去掉最后一个,
        if (result.endsWith("and ")) {
            return result.substring(0, result.length() -4);
        }
        return result;

    }

    public static String getProperty3(Map<String, String> property){
        String result = "";
        for (Map.Entry<String, String> entry : property.entrySet()) {
            result += "m." + entry.getKey() + "=" + "\"" + entry.getValue() + "\"" + ", ";
        }
        //去掉最后一个,
        if (result.endsWith(", ")) {
            return result.substring(0, result.length() -2);
        }
        return result;
    }


}
