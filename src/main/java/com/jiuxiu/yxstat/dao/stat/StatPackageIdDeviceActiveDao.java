package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatPackageIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao ;

    private StatPackageIdDeviceActiveDao() {}

    public static StatPackageIdDeviceActiveDao getInstance(){
        if(statPackageIdDeviceActiveDao == null){
            statPackageIdDeviceActiveDao = new StatPackageIdDeviceActiveDao();
        }
        return statPackageIdDeviceActiveDao;
    }

    public int savePackageIdStartUpCount(Object[] obj){
        String sql = "insert  into stat_package_id_device_active (package_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count=? ";
        return executeSql(sql , obj);
    }

    public int savePackageIdNewDeviceCount(Object[] obj){
        String sql = "insert  into stat_package_id_device_active (package_id , new_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update new_device_count=? ";
        return executeSql(sql , obj);
    }

    public int savePackageIdDeviceStartUpCount(Object[] obj){
        String sql = "insert  into stat_package_id_device_active (package_id , startup_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_device_count=? ";
        return executeSql(sql , obj);
    }




}
