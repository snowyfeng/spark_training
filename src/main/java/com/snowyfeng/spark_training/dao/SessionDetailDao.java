package com.snowyfeng.spark_training.dao;

import com.snowyfeng.spark_training.domains.SessionDetail;

import java.util.List;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public interface SessionDetailDao {
    void BatchInsert(List<SessionDetail> sessionDetailList);

    void insert(SessionDetail sessionDetail);
}
