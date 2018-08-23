package com.nextjoy.retention.utils;

import java.io.IOException;
import java.util.Properties;


/**
 *   Created with IDEA by chouFy on 2018/5/11.
 *   @author Zhoufy
 */
public class PropertyUtils {
    private Properties pro = new Properties();
    private static PropertyUtils propertyUtils = new PropertyUtils();

    private PropertyUtils() {
        try {
            pro.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("config.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e.getMessage());
        }

    }

    public static PropertyUtils getInstance(){
        return propertyUtils;
    }

    public static  String getValue(String key) {
        return (String)getInstance().pro.get(key);
    }
}

