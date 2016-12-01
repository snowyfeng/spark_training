package com.snowyfeng.spark_training.test;

import com.snowyfeng.spark_training.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class JDBCHelperTest {
    public static void main(String[] args) {
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        String sqlInsert = "insert into test_user values(?,?)";
        Object[] params = new Object[]{"张三", "234"};
        jdbcHelper.executeUpdate(sqlInsert, params);

        String sqlSelect = "select * from test_user";

        jdbcHelper.executeQuerry(sqlSelect, null, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws SQLException {
                while (rs.next()) {
                    String name = rs.getString(1);
                    String password = rs.getString(2);
                    System.out.println("name:"+name+","+"password:"+password);
                }

            }
        });


    }
}
