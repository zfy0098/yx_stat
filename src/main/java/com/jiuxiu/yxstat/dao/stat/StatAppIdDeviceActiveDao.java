package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

public class StatAppIdDeviceActiveDao extends StatDataBase implements Serializable{

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao;

    private StatAppIdDeviceActiveDao(){}

    public static StatAppIdDeviceActiveDao getInstance(){
        if(statAppIdDeviceActiveDao == null){
            statAppIdDeviceActiveDao = new StatAppIdDeviceActiveDao();
        }
        return statAppIdDeviceActiveDao;
    }

    public int saveAppIdStartUpCount(Object[] obj){
        String sql = "insert  into stat_app_id_device_active (app_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count=? ";
        return executeSql(sql , obj);
    }

    public int saveAppIdNewDeviceCount(Object[] obj){
        String sql = "insert  into stat_app_id_device_active (app_id , new_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update new_device_count=? ";
        return executeSql(sql , obj);
    }

    public int saveAppIdDeviceStartUpCount(Object[] obj){
        String sql = "insert  into stat_app_id_device_active (app_id , startup_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_device_count=? ";
        return executeSql(sql , obj);
    }

}
