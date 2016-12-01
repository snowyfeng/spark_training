package com.snowyfeng.spark_training.conf;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by xuxuefeng on 2016/11/22.
 */
public class ConfigurationManager {
    private static Properties prop = new Properties();

    static {
        try {
            InputStream in = ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    public static String getString(String key) {
        return prop.getProperty(key);
    }

    public static Boolean getBoolean(String key) {
        return Boolean.valueOf(getString(key));
    }

    public static Long getLong(String key) {
        return Long.valueOf(getString(key));
    }

    public static int getInt(String key) {
        return Integer.valueOf(getString(key));
    }

}
