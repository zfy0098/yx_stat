package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

public class StatChildDeviceActiveDao extends StatDataBase implements Serializable{

    private static  StatChildDeviceActiveDao statChildDeviceActiveDao;

    private StatChildDeviceActiveDao (){}

    public static StatChildDeviceActiveDao getInstance(){
        if(statChildDeviceActiveDao == null){
            statChildDeviceActiveDao = new StatChildDeviceActiveDao();
        }
        return statChildDeviceActiveDao;
    }

    public int saveChildIdStartUpCount(Object[] obj){
        String sql = "insert  into stat_child_id_device_active (child_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count=? ";
        return executeSql(sql , obj);
}

    public int saveChildIdNewDeviceCount(Object[] obj){
        String sql = "insert  into stat_child_id_device_active (child_id , new_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update new_device_count=? ";
        return executeSql(sql , obj);
    }

    public int saveChildIdDeviceStartUpCount(Object[] obj){
        String sql = "insert  into stat_child_id_device_active (child_id , startup_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_device_count=? ";
        return executeSql(sql , obj);
    }

}
