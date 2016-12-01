package com.snowyfeng.spark_training.domains;

import java.io.Serializable;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class Task implements Serializable {

    private static final long serialVersionUID = -4377225469474077545L;
    private long taskId;
    private String taskName;
    private String createTime;
    private String startTime;
    private String finishTime;
    private String taksType;
    private String taskStatus;
    private String taskParam;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getFinishTime() {
        return finishTime;
    }

    public void setFinishTime(String finishTime) {
        this.finishTime = finishTime;
    }

    public String getTaksType() {
        return taksType;
    }

    public void setTaksType(String taksType) {
        this.taksType = taksType;
    }

    public String getTaskStatus() {
        return taskStatus;
    }

    public void setTaskStatus(String taskStatus) {
        this.taskStatus = taskStatus;
    }

    public String getTaskParam() {
        return taskParam;
    }

    public void setTaskParam(String taskParam) {
        this.taskParam = taskParam;
    }
}
