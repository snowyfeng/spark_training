package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.TaskDao;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class DAOFactory {

    public static TaskDao getTaskDao(){
        return  new TaskImpl();
    }
}
