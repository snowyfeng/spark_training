package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.SessionRandomExtractDao;
import com.snowyfeng.spark_training.domains.SessionRandomExtract;
import com.snowyfeng.spark_training.jdbc.JDBCHelper;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class SessionRandomExtractDaoImpl implements SessionRandomExtractDao {
    @Override
    public void insert(SessionRandomExtract sessionRandomExtract) {
        String sql = "insert into session_random_extract values(?,?,?,?,?)";
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql, new Object[]{
                sessionRandomExtract.getTaskId(),
                sessionRandomExtract.getSessionId(),
                sessionRandomExtract.getStartTime(),
                sessionRandomExtract.getSeachKeywords(),
                sessionRandomExtract.getClickCategory()
        });

    }
}
