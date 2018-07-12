package com.jiuxiu.yxstat.dao.stat.deviceminute;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

public class StatAppIdMinuteDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatAppIdMinuteDeviceActiveDao statAppIdMinuteDeviceActiveDao = new StatAppIdMinuteDeviceActiveDao();

    private StatAppIdMinuteDeviceActiveDao() {
    }

    public synchronized static StatAppIdMinuteDeviceActiveDao getInstance() {
        return statAppIdMinuteDeviceActiveDao;
    }

    Object readResolve(){
        return statAppIdMinuteDeviceActiveDao;
    }

    /**
     * 保存app id 启动数
     *
     * @param
     * @return
     */
    public int saveAppIDMinuteStartUpCount(String time , String appid , int startupCount) {
        String tableName = "stat_app_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL,  new Object[]{appid, time});
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, startup_count, time) values (?,?,?) ";
            return executeSql(sql ,new Object[]{appid, startupCount, time});
        }else{
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and time = ?";
            return executeSql(sql, new Object[]{startupCount, appid, time});
        }
    }

    public int saveAppIdMinuteDeviceInstallCount(String time, int appID, long newDeviceCount, long startUpCount) {
        String tableName = "stat_app_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{appID, time});
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, new_device_count, startup_device_count, time) values (?,?,?,?)";
            return executeSql(sql , new Object[]{appID , newDeviceCount, startUpCount, time});
        }else{
            String sql = "update " + tableName + " set new_device_count=?, startup_device_count=? where app_id = ? and time = ?";
            return executeSql(sql, new Object[]{newDeviceCount, startUpCount, appID, time});
        }
    }
}
