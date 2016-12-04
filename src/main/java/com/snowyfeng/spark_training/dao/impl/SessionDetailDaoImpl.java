package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.SessionDetailDao;
import com.snowyfeng.spark_training.domains.SessionDetail;
import com.snowyfeng.spark_training.jdbc.JDBCHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class SessionDetailDaoImpl implements SessionDetailDao {
    @Override
    public void BatchInsert(List<SessionDetail> sessionDetailList) {

        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        List<Object[]> paramsList = new ArrayList<Object[]>();
        for (SessionDetail sessionDetail : sessionDetailList) {
            Object[] params = new Object[]{sessionDetail.getTaskId(),
                    sessionDetail.getUserId(),
                    sessionDetail.getSessionId(),
                    sessionDetail.getPageId(),
                    sessionDetail.getActionTime(),
                    sessionDetail.getSearchKeyword(),
                    sessionDetail.getClickCategoryId(),
                    sessionDetail.getClickPrductId(),
                    sessionDetail.getOrderCategoryId(),
                    sessionDetail.getOrderProductId(),
                    sessionDetail.getPayCategoryId(),
                    sessionDetail.getPayProductId()};
            paramsList.add(params);
        }
        jdbcHelper.executebatch(sql, paramsList);


    }

    @Override
    public void insert(SessionDetail sessionDetail) {

        String sql = "insert into session_detail values(?,?,?,?,?,?,?,?,?,?,?,?)";

        JDBCHelper jdbcHelper = JDBCHelper.getInstance();

        jdbcHelper.executeUpdate(sql,new Object[]{
                sessionDetail.getUserId(),
                sessionDetail.getSessionId(),
                sessionDetail.getPageId(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickPrductId(),
                sessionDetail.getOrderCategoryId(),
                sessionDetail.getOrderProductId(),
                sessionDetail.getPayCategoryId(),
                sessionDetail.getPayProductId()
        });


    }
}
