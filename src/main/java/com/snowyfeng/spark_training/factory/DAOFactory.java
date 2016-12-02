package com.snowyfeng.spark_training.factory;

import com.snowyfeng.spark_training.dao.*;
import com.snowyfeng.spark_training.dao.impl.*;

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

    public static SessionRandomExtractDao getSessionRandomExtractDao() {
        return new SessionRandomExtractDaoImpl();
    }

    public static SessionDetailDao getSessionDetailDao() {
        return new SessionDetailDaoImpl();
    }

    public static Top10CategoryDao getTop10CategoryDao() {
        return new Top10CategoryDaoImpl();
    }

    public static Top10CategorySessionDao getTop10CategorySessionDao() {
        return new Top10CategorySessionDaoImpl();
    }
}
