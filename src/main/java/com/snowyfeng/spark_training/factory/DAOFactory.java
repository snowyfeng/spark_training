package com.snowyfeng.spark_training.factory;

import com.snowyfeng.spark_training.dao.SessionAggrStatDao;
import com.snowyfeng.spark_training.dao.TaskDao;
import com.snowyfeng.spark_training.dao.impl.SessionAggrStatDaoImpl;
import com.snowyfeng.spark_training.dao.impl.TaskImpl;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class DAOFactory {

    public static TaskDao getTaskDao(){
        return  new TaskImpl();
    }

    public static SessionAggrStatDao getSessionAggrStatDao() {
        return new SessionAggrStatDaoImpl();
    }
}