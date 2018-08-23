package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * @author hadoop
 */
public class StatAppIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = new StatAppIdDeviceActiveDao();

    private StatAppIdDeviceActiveDao() {
    }

    public synchronized static StatAppIdDeviceActiveDao getInstance() {
        return statAppIdDeviceActiveDao;
    }


    Object readResolve() {
        return statAppIdDeviceActiveDao;
    }

    /**
     * 保存app id 启动数
     *
     * @param
     * @return
     */
    public int saveAppIdStartUpCount(String date, String appid, int startupCount) {
        String querySQL = "select id from stat_app_id_device_active where app_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_app_id_device_active (app_id, startup_count, date) values (?,?,?) ";
            return executeSql(sql, new Object[]{appid, startupCount, date});
        } else {
            String sql = "update stat_app_id_device_active set startup_count = ifnull(startup_count,0) + ? where app_id = ? and date = ?";
            return executeSql(sql, new Object[]{startupCount, appid, date});
        }
    }

    /**
     * 保存 app id 设备新增和设备启动
     *
     * @return
     */
    public int saveAppIdDeviceInstallCount(String date, int appid, long newDeviceCount, long startupDeviceCount) {

        String querySQL = "select id from stat_app_id_device_active where app_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, date});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert into stat_app_id_device_active (app_id, new_device_count, startup_device_count, date) values (?,?,?,?) ";
            return executeSql(insertSQL, new Object[]{appid, newDeviceCount, startupDeviceCount, date});
        } else {
            String updateSQL = "update stat_app_id_device_active set new_device_count = ?, startup_device_count=? where app_id = ? and date = ? ";
            return executeSql(updateSQL, new Object[]{newDeviceCount, startupDeviceCount, appid, date});
        }
    }



    /**
     * 保存app id  分钟 启动数
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

    /**
     *  保存 app id 分钟 设备数
     * @param time
     * @param appID
     * @param newDeviceCount
     * @param startUpCount
     * @return
     */
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
