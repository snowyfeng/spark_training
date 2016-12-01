package com.snowyfeng.spark_training.jdbc;

import com.snowyfeng.spark_training.conf.ConfigurationManager;
import com.snowyfeng.spark_training.constants.Constants;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.ArrayUtils;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class JDBCHelper {
    private static JDBCHelper instance = null;
    private static LinkedList<Connection> dataSource = new LinkedList<Connection>();

    static {
        try {
            String driver = ConfigurationManager.getString(Constants.JDBC_DRIVER);
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public JDBCHelper() {
        int dataSourceSize = ConfigurationManager.getInt(Constants.JDBC_DATASOURCE_SIZE);
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        String url = null;
        String user = null;
        String password = null;
        if (local) {
            url = ConfigurationManager.getString(Constants.JDBC_URL);
            user = ConfigurationManager.getString(Constants.JDBC_USER);
            password = ConfigurationManager.getString(Constants.JDBC_PASSWORD);
        } else {
            url = ConfigurationManager.getString(Constants.JDBC_URL_PROD);
            user = ConfigurationManager.getString(Constants.JDBC_USER_PROD);
            password = ConfigurationManager.getString(Constants.JDBC_PASSWORD_PROD);
        }
        for (int i = 0; i < dataSourceSize; i++) {
            try {
                Connection conn = DriverManager.getConnection(url, user, password);
                dataSource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static JDBCHelper getInstance() {
        if (instance == null) {
            synchronized (JDBCHelper.class) {
                if (instance == null) {
                    return new JDBCHelper();
                }
            }
        }
        return instance;
    }


    public static Connection getConnection() {
        while (dataSource.size() == 0) {
            try {
                Thread.sleep(100L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return dataSource.poll();
    }


    public static int executeUpdate(String sql, Object[] params) {
        Connection conn = null;
        PreparedStatement ps = null;
        int rst = 0;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            if (!ArrayUtils.isEmpty(params)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            rst = ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rst;
    }

    public static void executeQuerry(String sql, Object[] params, QueryCallback callback) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            if (!ArrayUtils.isEmpty(params)) {
                for (int i = 0; i < params.length; i++) {
                    ps.setObject(i + 1, params[i]);
                }
            }
            rs = ps.executeQuery();
            callback.process(rs);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }

    }


    public static int[] executebatch(String sql, List<Object[]> paramsList) {
        Connection conn = null;
        PreparedStatement ps = null;
        int[] rst = null;
        try {
            conn = getConnection();
            ps = conn.prepareStatement(sql);
            if (CollectionUtils.isNotEmpty(paramsList)) {
                for (Object[] params : paramsList) {
                    if (!ArrayUtils.isEmpty(params)) {
                        for (int i = 0; i < params.length; i++) {
                            ps.setObject(i + 1, params[i]);
                        }
                    }
                }
                ps.addBatch();
            }
            rst = ps.executeBatch();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (conn != null) {
                dataSource.push(conn);
            }
        }
        return rst;
    }


     public interface QueryCallback {
        void process(ResultSet rs) throws SQLException;
    }


}
