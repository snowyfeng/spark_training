package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.Top10CategorySessionDao;
import com.snowyfeng.spark_training.domains.Top10CategorySession;
import com.snowyfeng.spark_training.jdbc.JDBCHelper;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class Top10CategorySessionDaoImpl implements Top10CategorySessionDao {
    @Override
    public void insert(Top10CategorySession top10CategorySession) {
        String sql = "insert into top10_category_session values(?,?,?,?)";

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeUpdate(sql, new Object[]{
                top10CategorySession.getTaskId(),
                top10CategorySession.getCategoryId(),
                top10CategorySession.getSessionId(),
                top10CategorySession.getClickCount()
        });
    }
}
