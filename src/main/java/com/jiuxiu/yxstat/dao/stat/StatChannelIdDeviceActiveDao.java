package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatChannelIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao ;

    private StatChannelIdDeviceActiveDao() {}

    public static StatChannelIdDeviceActiveDao getInstance(){
        if(statChannelIdDeviceActiveDao == null){
            statChannelIdDeviceActiveDao = new StatChannelIdDeviceActiveDao();
        }
        return statChannelIdDeviceActiveDao;
    }

    public int saveChannelIdStartUpCount(Object[] obj){
        String sql = "insert  into stat_channel_id_device_active (channel_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count=? ";
        return executeSql(sql , obj);
    }

    public int saveChannelIdNewDeviceCount(Object[] obj){
        String sql = "insert  into stat_channel_id_device_active (channel_id , new_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update new_device_count=? ";
        return executeSql(sql , obj);
    }

    public int saveChannelIdDeviceStartUpCount(Object[] obj){
        String sql = "insert  into stat_channel_id_device_active (channel_id , startup_device_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_device_count=? ";
        return executeSql(sql , obj);
    }




}
