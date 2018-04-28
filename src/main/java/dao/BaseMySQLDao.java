package dao;

import org.apache.commons.dbcp.BasicDataSourceFactory;

import javax.sql.DataSource;
import java.io.InputStream;
import java.io.Serializable;
import java.sql.*;
import java.util.List;
import java.util.Properties;

/**
 * Created by Macan on 2018/2/24.
 *
 * MySQL 数据库连接池
 */
public class BaseMySQLDao implements Serializable {
    private static final String USER = "macan";
    private static final String PASSWORD ="macan";
    private static final String URL = "jdbc:mysql://210.40.16.118:3306/test_court_db&rewriteBatchedStatements=true";
    private static final String DRIVER_CLASS = "com.mysql.jdbc.Driver";

    protected Connection conn;
    protected PreparedStatement pstmt;
    protected Statement stmt;
    protected Statement statement;
    protected ResultSet rst;
    protected static DataSource ds = null;

    protected static int batchSize = 0;

    private List<String> sqls;

    static{
        try {
            //加载dbcpconfig.properties配置文件
            InputStream in = BaseDao.class.getClassLoader().getResourceAsStream("dbcpconfig.properties");
            Properties prop = new Properties();
            prop.load(in);
            //创建数据源
            ds = BasicDataSourceFactory.createDataSource(prop);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * 连接
     * @throws SQLException
     */
    protected Connection getConnection() throws SQLException{
//		conn = DriverManager.getConnection(URL, USER, PASSWORD);
        conn = ds.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * 关闭数据库连接的方法
     */
    protected void close(){
//		DBUtil.closeAll(conn, stmt, rst);
        if(null != rst){
            try {
                rst.close();
            } catch (SQLException e) {
                e.printStackTrace();
            } finally{
                rst=null;
            }
        }
        if(null != pstmt){
            try {
                pstmt.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally{
                pstmt=null;
            }
        }
        if(null != conn){
            try {
                conn.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            } finally{
                conn=null;
            }
        }
    }

    /**
     * 数据库更新的方法
     * @param sql
     * @param params
     * @return
     */
    protected final int executeUpdate(String sql, Object...params){
        int num = 0;
        try {
            this.getConnection();
            pstmt = conn.prepareStatement(sql);
            setPreparedStatement(params);
            num = pstmt.executeUpdate();
            conn.commit();
        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            this.close();
        }
        return num;
    }

    /**
     * 批量更新
     * @param sqls
     * @return
     */
    protected final void executeUpdate(List<String> sqls){
        int num = 0;
        try {
            this.getConnection();
            stmt = conn.createStatement();
            for (String sql : sqls) {
                stmt.addBatch(sql);
//                num++;
//                if (num % 5000 == 0) {
//
//                }
            }
            stmt.executeBatch();
            conn.commit();

        } catch (SQLException e) {
            e.printStackTrace();
        } finally{
            this.close();
        }
    }

    /**
     * 数据库查询的方法
     * @param sql
     * @param params
     * @return
     * @throws SQLException
     */
    protected final ResultSet executeQuery(
            String sql,Object...params) throws SQLException{
        this.getConnection();
        pstmt = conn.prepareStatement(sql);
        setPreparedStatement(params);
        rst = pstmt.executeQuery();
//		this.close();
        return rst;
    }


    private void setPreparedStatement(Object... params) throws SQLException {
        if(params!=null && params.length > 0){
            for(int i=0;  i<params.length; i++){
                pstmt.setObject(i + 1, params[i]);
            }
        }
    }

}
