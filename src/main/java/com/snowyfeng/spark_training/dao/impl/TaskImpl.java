package com.snowyfeng.spark_training.dao.impl;

import com.snowyfeng.spark_training.dao.TaskDao;
import com.snowyfeng.spark_training.domains.Task;
import com.snowyfeng.spark_training.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class TaskImpl implements TaskDao{

    @Override
    public Task findById(long taskId) {
        String sql ="select *from task where task_id=?";
        JDBCHelper jdbcHelper = JDBCHelper.getInstance();
        Object[] params = new Object[]{taskId};
        final Task task = new Task();
        jdbcHelper.executeQuerry(sql, params, new JDBCHelper.QueryCallback() {
            @Override
            public void process(ResultSet rs) throws SQLException {
               if(rs.next()){
                   task.setTaskName(rs.getString(2));
                   task.setCreateTime(rs.getString(3));
                   task.setStartTime(rs.getString(4));
                   task.setFinishTime(rs.getString(5));
                   task.setTaksType(rs.getString(6));
                   task.setTaksType(rs.getString(7));
                   task.setTaskParam(rs.getString(8));
               }
            }
        });
        return task;
    }
}
