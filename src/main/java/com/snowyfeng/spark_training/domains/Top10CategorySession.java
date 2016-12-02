package com.snowyfeng.spark_training.domains;

import java.io.Serializable;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class Top10CategorySession implements Serializable {
    private static final long serialVersionUID = -4554907469287900177L;

    private long taskId;
    private long categoryId;
    private String sessionId;
    private long clickCount;


    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(long categoryId) {
        this.categoryId = categoryId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }
}
