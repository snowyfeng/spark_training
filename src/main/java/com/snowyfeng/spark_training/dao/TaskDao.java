package com.snowyfeng.spark_training.dao;

import com.snowyfeng.spark_training.domains.Task;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public interface TaskDao {
    Task findById(long taskId);
}
