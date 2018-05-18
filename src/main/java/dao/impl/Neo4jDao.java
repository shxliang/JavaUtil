package dao.impl;

import dao.BaseDao;
import org.neo4j.driver.v1.Record;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Macan
 * @date 2018/1/12
 */
public class Neo4jDao extends BaseDao implements Serializable {
    /**
     * 创建一个节点
     *
     * @param label    节点标签列表
     * @param property 节点属性map, key:属性名称，value:属性值
     * @return 返回受影响的节点个数
     */
    public int createNode(List<String> label, Map<String, String> property) {
        // 根据传入的参数，创建sql语句
        String labels = getLabels(label);
        String properties = getProperty(property);

        String sql = "MERGE (m: ? {?})";

        return executeUpdate(sql, labels, properties);
    }

    /**
     * 创建节点
     *
     * @param labels   节点标签列表
     * @param property 节点属性map
     * @return 返回受影响的节点个数
     */
    public int createNode(String labels, String property) {
        String sql = "MERGE (m: ? {?})";
        int num = executeUpdate(sql, labels, property);
        return num;
    }

    /**
     * 删除节点
     *
     * @param label    label条件
     * @param property 属性条件
     * @param like     是否使用模糊查询
     * @return
     */
    public int removeNode(List<String> label, Map<String, String> property, boolean like) {
        String labels = getLabels(label);
        String properties = getProperty(property);
        return removeNode(labels, properties);
    }

    /**
     * 删除节点
     *
     * @param labels
     * @param property
     * @return
     */
    public int removeNode(String labels, String property) {
        String sql = "MATCH (m: ? {?}) DELETE m";
        int num = executeUpdate(sql, labels, property);
        return num;
    }

    /**
     * 判断节点是否存在
     *
     * @param property
     * @return
     * @throws SQLException
     */
    public boolean isNodeExits(Map<String, String> property) throws SQLException {
        String properties = getProperty2(property);
        return isNodeExits(properties);
    }

    public boolean isNodeExits(String property) throws SQLException {
        String sql = "match (m) where ? return m";
        rst = executeQuery(sql, property);
        while (rst.hasNext()) {
            Record record = rst.next();
            if (record.get("m") != null) {
                return true;
            }
        }
        return false;
    }

    /**
     * 创建关系，如果两个节点不存在，将创建节点并且创建关系
     *
     * @param label1
     * @param label2
     * @param property1
     * @param property2
     * @return
     */
    public int createRelation(List<String> label1,
                              List<String> label2,
                              Map<String, String> property1,
                              Map<String, String> property2,
                              String rlabel,
                              Map<String, String> rpro) throws SQLException {
        int num = -1;
        String label1s = getLabels(label1);
        String label2s = getLabels(label2);
        String pro1 = getProperty(property1);
        String pro2 = getProperty(property2);
        String rpros = getProperty(rpro);
        String sql = "MERGE (m:?{?}) MERGE (n:?{?}) MERGE (m)-[r:?{?}]->(n)";
        num = executeUpdate(sql, label1s, pro1, label2s, pro2, rlabel, rpros);
        return num;
    }

    public int createRelation(String label1, String label2, Map<String, String> property1, Map<String, String> property2,
                              String rlabel, String rpro) {
        String pro1 = getProperty(property1);
        String pro2 = getProperty(property2);
        String sql = "MERGE (m:?{?}) MERGE (n:?{?}) MERGE (m)-[r:?{?}]->(n)";
        return executeUpdate(sql, label1, pro1, label2, pro2, rlabel, rpro);
    }

    public int createRelation(String sql) {
        return executeUpdate(sql);
    }

    /**
     * 修改节点标签
     *
     * @param label
     * @param id
     * @param update
     * @return
     */
    public int updateNodeLabel(String label, String id, List<String> update) {
        String updates = getLabels(update);
        String sql = "merge (m:?{?}) on match set ?";
        int num = executeUpdate(sql, label, id, updates);
        return num;
    }

    /**
     * 修改节点属性,如果节点不存在则创建
     *
     * @param label
     * @param prpperty
     * @param update
     * @return
     */
    public int updateNodeProperty(String label, Map<String, String> prpperty, Map<String, String> update) {
        String sql = "merge (m:?{?}) on match set ?";
        String prppertys = getProperty(prpperty);
        String updates = getProperty3(update);
        int num = executeUpdate(sql, label, prppertys, updates);
        return num;
    }

    /**
     * 修改节点属性，如果节点不存在则忽略
     *
     * @param label
     * @param prpperty
     * @param update
     * @return
     */
    public int updateExitsNodeProperty(String label, Map<String, String> prpperty, Map<String, String> update) {
        String sql = "match (m:?{?})  set ?";
        String prppertys = getProperty(prpperty);
        String updates = getProperty3(update);
        int num = executeUpdate(sql, label, prppertys, updates);
        return num;
    }


    /**
     * 修改节点属性
     *
     * @param label
     * @param prpperty
     * @param update
     * @return
     */
    public int updateNodeProperty(String label, String prpperty, Map<String, String> update) {
        String sql = "merge (m:?{?}) on match set ?";
        String updates = getProperty3(update);
        int num = executeUpdate(sql, label, prpperty, updates);
        return num;
    }


    /**
     * 根据id 删除节点
     *
     * @param label
     * @param id
     * @return
     */
    public int deleteNode(String label, String id) {
        String sql = "match (m) where m.id=? delete m";
        int num = executeUpdate(sql, id);
        return num;
    }

    /**
     * 根据id 删除关系
     *
     * @param id1
     * @param id2
     * @param id3
     * @return
     */
    public int deleteRealtion(String id1, String id2, String id3) {
        String sql = "match (m), (n), (m)-[r]->(n) where m.id=? and n.id=? and r.id=?";
        int num = executeUpdate(sql, id1, id2, id3);
        return num;
    }

    /**
     * 根据ID 更新法官或者受害人
     *
     * @param label
     * @param key
     * @param update
     * @return
     */
    public int updatePersonByID(String label, String key, Map<String, String> update) {
        String sql = "merge (m:?{ID:?}) on match set ?";
        if (!key.startsWith("\"")) {
            key = "\"" + key + "\"";
        }
        String updates = getProperty3(update);
        return executeUpdate(sql, label, key, updates);
    }

    /**
     * 根据caseID, 更新案件信息
     *
     * @param caseID
     * @param update
     * @return
     */
    public int updateCaseByCaseID(String caseID, Map<String, String> update) {
        String sql = "merge (m:Case{caseID:?}) on match set ?";
        String updates = getProperty3(update);
        return executeUpdate(sql, caseID, updates);
    }

    /**
     * 根据两个节点的key , 创建两个节点的关系
     *
     * @param key1        案件的key
     * @param key2        受害人的key
     * @param relLabel    关系标签
     * @param relProperty 关系名称
     * @return
     */
    public int createCaseAndVictimRelation(String key1, String key2, String relLabel, String relProperty) {
        String sql = "create (m:Case{caseID:\"?\"})-[r:?{name:\"?\"}]->(n:Victim{ID:\"?\"})";
        return executeUpdate(sql, key1, relLabel, relProperty, key2);
    }

    /**
     * 更新案件与原被告的关系
     *
     * @param caseID
     * @param idCard
     * @param property
     * @param relLabel
     * @param relProperty
     * @return
     */
    public int createCaseAndAcc(String caseID, String idCard, Map<String, String> property, String relLabel, String relProperty) {
        String sql = "create (m:Case{caseID:\"?\"})-[r:?{name:\"?\"}]->(n:Person{IDCard:\"?\"})";
        return executeUpdate(sql, caseID, relLabel, relProperty, idCard);
    }
}
