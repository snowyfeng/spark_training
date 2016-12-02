package com.snowyfeng.spark_training.domains;

import java.io.Serializable;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class SessionRandomExtract implements Serializable {


    private static final long serialVersionUID = -5213474627272749345L;
    private long taskId;
    private String sessionId;
    private String startTime;
    private String seachKeywords;
    private String clickCategory;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getSeachKeywords() {
        return seachKeywords;
    }

    public void setSeachKeywords(String seachKeywords) {
        this.seachKeywords = seachKeywords;
    }

    public String getClickCategory() {
        return clickCategory;
    }

    public void setClickCategory(String clickCategory) {
        this.clickCategory = clickCategory;
    }
}
