package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatAppChannelIdDeviceActiveDao  extends StatDataBase implements Serializable {

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao ;

    private StatAppChannelIdDeviceActiveDao() {}

    public static StatAppChannelIdDeviceActiveDao getInstance(){
        if(statAppChannelIdDeviceActiveDao == null){
            statAppChannelIdDeviceActiveDao = new StatAppChannelIdDeviceActiveDao();
        }
        return statAppChannelIdDeviceActiveDao;
    }

    public int saveAppChannelIdStartUpCount(Object[] obj){
        String sql = "insert  into stat_app_channel_id_device_active (app_channel_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count=? ";
        return executeSql(sql , obj);
    }

    public int saveAppChannelIdNewDeviceCount(Object[] obj){
        String sql = "insert  into stat_app_channel_id_device_active (app_channel_id , new_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update new_device_count=? ";
        return executeSql(sql , obj);
    }

    public int saveAppChannelIdDeviceStartUpCount(Object[] obj){
        String sql = "insert  into stat_app_channel_id_device_active (app_channel_id , startup_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_device_count=? ";
        return executeSql(sql , obj);
    }




}
