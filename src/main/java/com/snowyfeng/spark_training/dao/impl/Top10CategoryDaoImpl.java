package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.Top10CategoryDao;
import com.snowyfeng.spark_training.domains.Top10Category;
import com.snowyfeng.spark_training.jdbc.JDBCHelper;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class Top10CategoryDaoImpl implements Top10CategoryDao {
    @Override
    public void insert(Top10Category top10Category) {
        String sql = "insert into top10_category values(?,?,?,?,?)";

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeUpdate(sql, new Object[]{
                top10Category.getTaskId(),
                top10Category.getCategoryId(),
                top10Category.getClickCount(),
                top10Category.getOrderCount(),
                top10Category.getPayCount()
        });

    }
}
