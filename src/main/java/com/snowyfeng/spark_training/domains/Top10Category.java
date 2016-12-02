package com.snowyfeng.spark_training.domains;

import java.io.Serializable;

/**
 * Created by xuxuefeng on 2016-12-2.
 */
public class Top10Category implements Serializable {

    private static final long serialVersionUID = -7399923997315054457L;

    private long taskId;
    private long categoryId;
    private long clickCount;
    private long orderCount;
    private long payCount;

    public long getPayCount() {
        return payCount;
    }

    public void setPayCount(long payCount) {
        this.payCount = payCount;
    }

    public long getOrderCount() {
        return orderCount;
    }

    public void setOrderCount(long orderCount) {
        this.orderCount = orderCount;
    }

    public long getClickCount() {
        return clickCount;
    }

    public void setClickCount(long clickCount) {
        this.clickCount = clickCount;
    }

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


}
