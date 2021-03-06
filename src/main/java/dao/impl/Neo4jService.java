package dao.impl;//package dao.impl;
//
//import dao.BaseDao;
//
//import java.io.Serializable;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.List;
//import java.util.Map;
//
///**
// * Created by Macan on 2018/1/2.
// */
//public class Neo4jService extends BaseDao implements Serializable{
//
//    /**
//     * 创建一个节点
//     * @param label 节点标签列表
//     * @param property 节点属性map, key:属性名称，value:属性值
//     * @return 返回受影响的节点个数
//     */
//    public int createNode(List<String> label, Map<String, String> property) {
//        //根据传入的参数，创建sql语句
//        String labels = getLabels(label);
//        String properties = getProperty(property);
//        return createNode(labels, properties);
//    }
//
//    /**
//     * 创建节点
//     * @param labels 节点标签列表
//     * @param property 节点属性map
//     * @return 返回受影响的节点个数
//     */
//    public int createNode(String labels, String property) {
////        String sql = "CREATE (m:"+ labels+ "{ "+ property +"})";
//        String sql = "MERGE (m: ? {?})";
//        int num = executeUpdate(sql, labels, property);
//        return num;
//    }
//
//    /**
//     * 删除节点
//     * @param label label条件
//     * @param property  属性条件
//     * @param like 是否使用模糊查询
//     * @return
//     */
//    public int removeNode(List<String> label, Map<String, String> property, boolean like) {
//        String labels = getLabels(label);
//        String properties = getProperty(property);
//        return removeNode(labels, properties);
//    }
//
//    /**
//     * 删除节点
//     * @param labels
//     * @param property
//     * @return
//     */
//    public int removeNode(String labels, String property) {
//        String sql = "MATCH (m: ? {?}) DELETE m";
//        int num = executeUpdate(sql, labels, property);
//        return num;
//    }
//
//    /**
//     * 判断节点是否存在
//     * @param property
//     * @return
//     * @throws SQLException
//     */
//    public boolean isNodeExits(Map<String, String> property) throws SQLException {
//        String properties = getProperty2(property);
//        return isNodeExits(properties);
//    }
//
//    public boolean isNodeExits(String property) throws SQLException {
//        String sql = "match (m) where ? return count(m) ";
//        rst = executeQuery(sql, property);
//        if (rst.next() && !rst.getString("count(m)").equals("0")) {
//            return true;
//        }
//        return false;
//    }
//
//    /**
//     * 判断关系是否存在
//     * @param rela
//     * @return
//     */
//    public boolean isRelationExits(String rela) {
//        return false;
//    }
//
//    /**
//     * 创建关系，如果两个节点不存在，将创建节点并且创建关系
//     * @param label1
//     * @param label2
//     * @param property1
//     * @param property2
//     * @return
//     */
//    public int createRelation(List<String> label1, List<String> label2, Map<String, String> property1, Map<String, String> property2,
//                              String rlabel, Map<String, String> rpro) throws SQLException {
//        //match (m:Person:Judgement{name:"周志华2",age:"20"}), (n:Person:Judgement{name:"张三2",age:"30"}) merge (m)-[r:Friend{name:"朋友"}]->(n)
//
//        int num = -1;
//        //如果节点1,或者节点2不存在，创建node1,node2
////        boolean boolNode1 = isNodeExits(property1);
////        boolean boolNode2 = isNodeExits(property2);
////        if (!boolNode1 || !boolNode2){
////            if (! isNodeExits(property1)) {
////                createNode(label1, property1);
////            }else {
////                createNode(label2, property2);
////            }
////        }
//
//        String label1s = getLabels(label1);
//        String label2s = getLabels(label2);
//        String pro1 = getProperty(property1);
//        String pro2 = getProperty(property2);
//        String rpros = getProperty(rpro);
//        String sql = "MERGE (m:?{?}) MERGE (n:?{?}) MERGE (m)-[r:?{?}]->(n)";
//        num = executeUpdate(sql, label1s, pro1, label2s, pro2, rlabel, rpros);
//
//
//        return num;
//    }
//
//
//    public int createRelation(String sql) {
//        // merge (m:person{name:"周志华",age:"20"}),(m:person{name:"周志华",age:"20"}), (m)-[r:Links{}]->(n)
//        return executeUpdate(sql);
//    }
//
//
//    /**
//     * 检查关系是否存在
//     * @param label1 实体2 label
//     * @param label2 实体2 lablel
//     * @param property1 唯一标示实体1 的属性，如身份证ID，
//     * @param property2
//     * @param rlabel 关系标签
//     * @param rpro 唯一标示关系的属性名
//     * @return 存在返回true, 否则false
//     * @throws SQLException
//     */
//    public boolean isRelationExits(String label1, String label2, String property1, String property2,
//                               String rlabel, String rpro) throws SQLException {
//        //首先检查关系是否存在
//        String sql = "match (m: ? { ? }), (n:?{?}), (m)-[r:?{?}]->(n) return count(r)";
//        rst = executeQuery(sql, label1, property1, label2, property2, rlabel, rpro);
//        if (rst.next() && !rst.getString("count(r)").equals("0")) {
//            return true;
//        }
//        return false;
//    }
//
//    public boolean isRelationExits(List<String> label1, List<String> label2, Map<String, String> property1, Map<String, String> property2,
//                                   String rlabel, Map<String, String> rpro) throws SQLException {
//        //首先检查关系是否存在
//
//        String label1s = getLabels(label1);
//        String label2s = getLabels(label2);
//        String pro1 = getProperty(property1);
//        String pro2 = getProperty(property2);
//        String rpros = getProperty(rpro);
//        return isRelationExits(label1s, label2s, pro1, pro2, rlabel, rpros);
//
//    }
//    /**
//     * 移除关系
//     * @param label1 实体2 label
//     * @param label2 实体2 lablel
//     * @param property1 唯一标示实体1 的属性，如身份证ID，
//     * @param property2
//     * @param rlabel 关系标签
//     * @param rpro 唯一标示关系的属性名
//     * @return
//     */
//    public int removeRelation(List<String> label1, List<String> label2, Map<String, String> property1, Map<String, String> property2,
//                              String rlabel, Map<String, String> rpro) throws SQLException {
//        //首先检查关系是否存在
//        boolean flag = isRelationExits(label1, label2, property1, property2, rlabel, rpro);
//        //如果关系已经存在了,那么移除关系
//        int num = -1;
//        if (flag) {
//            String sql = "match (m: ? { ? }), (n:?{?}), (m)-[r:?{?}]->(n) delete r";
//            num = executeUpdate(sql, label1, property1, label2, property2, rlabel, rpro);
//        }
//        return num;
//    }
//
//    public int removeRelation(String label1, String label2, String property1, String property2,
//                              String rlabel, String rpro) throws SQLException {
//        //首先检查关系是否存在
//        boolean flag = isRelationExits(label1, label2, property1, property2, rlabel, rpro);
//        //如果关系已经存在了,那么移除关系
//        int num = -1;
//        if (flag) {
//            String sql = "match (m: ? { ? }), (n:?{?}), (m)-[r:?{?}]->(n) delete r";
//            num = executeUpdate(sql, label1, property1, label2, property2, rlabel, rpro);
//        }
//        return num;
//    }
//
//
//    /**
//     * 查找节点
//     * @param label 节点标签
//     * @param property 节点属性
//     * @return
//     */
//    public ResultSet findNode(String label, String property) {
//        String sql = "match (m:?) where ? return m";
//        rst = executeQuery(sql, label, property);
//        return rst;
//    }
//
//    /**
//     * 查找关系
//     * @param label1 节点1的标签
//     * @param label2 节点2的标签
//     * @param property1 节点1的属性
//     * @param property2 节点2的属性
//     * @param rlabel 关系属性
//     * @param rp 关系标签
//     * @return
//     */
//    public ResultSet findRelation(String label1, String label2, String property1, String property2, String rlabel, String rp) {
//        String sql = "match (m:?{?}), (n:?{?}), (m)-[r:?{?}]->(n) return m,n,r";
//        rst = executeQuery(sql, label1, property1, label2, property2, rlabel, rp);
//        return rst;
//    }
//
//    /**
//     * 修改节点标签
//     * @param label
//     * @param id
//     * @param update
//     * @return
//     */
//    public int updateNodeLabel(String label, String id, List<String> update) {
//        String updates = getLabels(update);
//        String sql = "merge (m:?{?}) on match set ?";
//        int num = executeUpdate(sql, label, id, updates);
//        return num;
//    }
//
//    /**
//     * 修改节点属性
//     * @param label
//     * @param prpperty
//     * @param update
//     * @return
//     */
//    public int updateNodeProperty(String label, Map<String, String> prpperty, Map<String, String> update) {
//        String sql = "merge (m:?{?}) on match set ?";
//        String prppertys = getProperty(prpperty);
//        String updates = getProperty2(update);
//        int num = executeUpdate(sql, label, prppertys, updates);
//        return num;
//    }
//
//    /**
//     * 根据id 删除节点
//     * @param label
//     * @param id
//     * @return
//     */
//    public int deleteNode(String label, String id) {
//        String sql = "match (m) where m.id=? delete m";
//        int num = executeUpdate(sql, id);
//        return num;
//    }
//
//    /**
//     * 根据id 删除关系
//     * @param id1
//     * @param id2
//     * @param id3
//     * @return
//     */
//    public int deleteRealtion(String id1, String id2, String id3) {
//        String sql = "match (m), (n), (m)-[r]->(n) where m.id=? and n.id=? and r.id=?";
//        int num = executeUpdate(sql, id1, id2, id3);
//        return num;
//    }
//}
