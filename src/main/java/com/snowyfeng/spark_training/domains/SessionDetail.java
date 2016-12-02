package com.snowyfeng.spark_training.domains;

import java.io.Serializable;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class SessionDetail implements Serializable {
    private static final long serialVersionUID = 6252157585901007065L;

    private long taskId;
    private long UserId;
    private String sessionId;
    private Long pageId;
    private String actionTime;
    private String searchKeyword;
    private long clickCategoryId;
    private long clickPrductId;
    private String orderCategoryId;
    private String orderProductId;
    private String payCategoryId;
    private String payProductId;

    public long getTaskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    public long getUserId() {
        return UserId;
    }

    public void setUserId(long userId) {
        UserId = userId;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public Long getPageId() {
        return pageId;
    }

    public void setPageId(Long pageId) {
        this.pageId = pageId;
    }

    public String getActionTime() {
        return actionTime;
    }

    public void setActionTime(String actionTime) {
        this.actionTime = actionTime;
    }

    public String getSearchKeyword() {
        return searchKeyword;
    }

    public void setSearchKeyword(String searchKeyword) {
        this.searchKeyword = searchKeyword;
    }

    public long getClickCategoryId() {
        return clickCategoryId;
    }

    public void setClickCategoryId(long clickCategoryId) {
        this.clickCategoryId = clickCategoryId;
    }

    public long getClickPrductId() {
        return clickPrductId;
    }

    public void setClickPrductId(long clickPrductId) {
        this.clickPrductId = clickPrductId;
    }

    public String getOrderCategoryId() {
        return orderCategoryId;
    }

    public void setOrderCategoryId(String orderCategoryId) {
        this.orderCategoryId = orderCategoryId;
    }

    public String getOrderProductId() {
        return orderProductId;
    }

    public void setOrderProductId(String orderProductId) {
        this.orderProductId = orderProductId;
    }

    public String getPayCategoryId() {
        return payCategoryId;
    }

    public void setPayCategoryId(String payCategoryId) {
        this.payCategoryId = payCategoryId;
    }

    public String getPayProductId() {
        return payProductId;
    }

    public void setPayProductId(String payProductId) {
        this.payProductId = payProductId;
    }
}
