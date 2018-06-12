package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

public class StatDeviceActiveDao extends StatDataBase implements Serializable {


    private static StatDeviceActiveDao statDeviceActiveDao = null;


    private StatDeviceActiveDao (){}


    public static StatDeviceActiveDao getInstance(){
        if(statDeviceActiveDao == null){
            statDeviceActiveDao = new StatDeviceActiveDao();
        }
        return statDeviceActiveDao;
    }


    public int saveDeviceActiveCount(Object[] obj){
        String sql = "insert  into stat_device_active (new_device_count, startup_device_count, startup_count, os, date) values  (?,?,?,?,date(now())) on  " +
                " duplicate key update  new_device_count=?, startup_device_count=?, startup_count=? ";
        return executeSql(sql , obj);
    }

}
