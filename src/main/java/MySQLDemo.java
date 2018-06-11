import util.FileUtil;

import java.sql.*;
import java.util.List;

/**
 * JDBC操作MySQL
 *
 * @author lsx
 * @date 2018/5/18
 */
public class MySQLDemo {
    static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
    static final String DB_URL = "jdbc:mysql://rm-uf65d386n0372w0tsmo.mysql.rds.aliyuncs.com:3306/propaganda_culture_cloud_data_supply?useUnicode=true&characterEncoding=utf8&autoReconnect=true";
    static final String USER = "gykj_mysql";
    static final String PASS = "8y%#oSHbodJRlVsCYgvB";

    public static void main(String[] args) {
        try {
            Class.forName(JDBC_DRIVER);
            Connection connection = DriverManager.getConnection(DB_URL, USER, PASS);
            connection.setAutoCommit(false);
            Statement statement = connection.createStatement();

            List<String> lines = FileUtil.readLocalFile("D:\\Downloads\\yqt\\topic.txt");
            StringBuilder filterSql = new StringBuilder();
            filterSql.append("(");

            for (String line : lines) {
                String[] parts = line.split("\t");
                if (parts.length == 2) {
                    statement.addBatch("UPDATE dataman_knowtoop1_piont_drop " +
                            "SET value=" + ("1".equals(parts[1]) ? "30" : "20") + " " +
                            "WHERE name='" + parts[0] + "'");
                } else {
                    statement.addBatch("UPDATE dataman_knowtoop1_piont_drop " +
                            "SET value=15 " +
                            "WHERE name='" + parts[0] + "'");
                }
                filterSql.append("'").append(parts[0]).append("'").append(",");
            }
            filterSql.deleteCharAt(filterSql.lastIndexOf(","));
            filterSql.append(")");

            statement.addBatch("UPDATE dataman_knowtoop1_piont_drop " +
                    "SET value=10 " +
                    "WHERE name NOT IN " + filterSql.toString() + " " +
                    "AND type=1");

            statement.executeBatch();
            connection.commit();
            statement.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
