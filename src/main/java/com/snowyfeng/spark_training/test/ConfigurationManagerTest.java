package com.snowyfeng.spark_training.test;

import com.snowyfeng.spark_training.conf.ConfigurationManager;
import com.snowyfeng.spark_training.constants.Constants;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class ConfigurationManagerTest {
    public static void main(String[] args) {
        String url = ConfigurationManager.getString(Constants.JDBC_URL);
        System.out.println(url);
    }
}
