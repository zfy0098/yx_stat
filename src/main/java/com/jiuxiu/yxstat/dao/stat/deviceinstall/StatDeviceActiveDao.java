package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * @author zhoufy
 */
public class StatDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatDeviceActiveDao statDeviceActiveDao = new StatDeviceActiveDao();

    private StatDeviceActiveDao (){}

    public synchronized static StatDeviceActiveDao getInstance(){
        return statDeviceActiveDao;
    }


    Object readResolve(){
        return statDeviceActiveDao;
    }

    public int saveDeviceActiveCount(String date ,long newDeviceCount, long deviceStartUpCount, long startUpCount, int os){

        String querySQL = "select id from stat_device_active where date = ? and os = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{date , os});
        if(map == null || map.isEmpty()){
            String sql = "insert into stat_device_active (new_device_count, startup_device_count, startup_count, os, date) values (?,?,?,?,?)";
            return executeSql(sql , new Object[]{newDeviceCount, deviceStartUpCount, startUpCount, os, date});
        }else{
            String sql = "update stat_device_active set new_device_count=?, startup_device_count=?, startup_count=? where date = ? and os = ?";
            return executeSql(sql , new Object[]{newDeviceCount, deviceStartUpCount, startUpCount, date, os });
        }
    }

    public int saveMinuteDeviceActiveCount(String time , long newDeviceCount, long deviceStartUpCount , long startUpCount , int os){
        String querySQL = "select id from stat_device_active_minute where time = ? and os = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{time , os});
        if(map == null || map.isEmpty()){
            String sql = "insert into stat_device_active_minute (new_device_count, startup_device_count, startup_count, os, time) values (?,?,?,?,?)";
            return executeSql(sql , new Object[]{newDeviceCount, deviceStartUpCount, startUpCount, os, time});
        }else{
            String sql = "update stat_device_active_minute set new_device_count=?, startup_device_count=?, startup_count=? where time = ? and os = ?";
            return executeSql(sql , new Object[]{newDeviceCount, deviceStartUpCount, startUpCount, time, os });
        }
    }
}
