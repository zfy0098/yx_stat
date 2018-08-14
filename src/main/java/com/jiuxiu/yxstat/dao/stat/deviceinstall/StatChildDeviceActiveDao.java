package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

public class StatChildDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = new StatChildDeviceActiveDao();

    private StatChildDeviceActiveDao() {
    }

    public synchronized static StatChildDeviceActiveDao getInstance() {
        return statChildDeviceActiveDao;
    }

    Object readResolve() {
        return statChildDeviceActiveDao;
    }

    /**
     * 保存 child id 启动数
     *
     * @return
     */
    public int saveChildIDStartUpCount(String date, String appID, String childID, int startupCount) {

        String querySQL = "SELECT id from stat_child_id_device_active where app_id = ? and child_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_child_id_device_active (app_id, child_id, startup_count, date) values (?,?,?,?)";
            return executeSql(sql, new Object[]{appID, childID, startupCount, date});
        } else {
            String sql = "update stat_child_id_device_active set startup_count = ifnull(startup_count,0) + ? where  app_id = ? and child_id = ? and date = ? ";
            return executeSql(sql, new Object[]{startupCount, appID, childID, date});
        }
    }

    /**
     * 保存 child id 设备新增和设备启动数
     *
     * @return
     */
    public int saveChildIdDeviceInstallCount(String date, int appID, int childID, long newDeviceCount, long startupDeviceCount) {

        String querySQL = "select id from stat_child_id_device_active where app_id = ? and child_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_child_id_device_active (app_id, child_id, new_device_count, startup_device_count, date) values (?,?,?,?,?)";
            return executeSql(sql, new Object[]{appID, childID, newDeviceCount, startupDeviceCount, date});
        } else {
            String sql = "update stat_child_id_device_active set new_device_count = ? , startup_device_count = ? where app_id = ? and child_id = ? and date = ?";
            return executeSql(sql, new Object[]{newDeviceCount, startupDeviceCount, appID, childID, date});
        }
    }



    /**
     * 保存 child id 启动数
     *
     * @return
     */
    public int saveChildIDMinuteStartUpCount(String time , String appID, String childID , int startupCount) {
        String tableName = "stat_child_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "SELECT id from " + tableName + " where app_id = ? and child_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL, new Object[]{appID, childID, time});
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, child_id, startup_count, time) values (?,?,?,?)";
            return  executeSql(sql , new Object[]{appID, childID, startupCount, time});
        }else{
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and time = ? ";
            return executeSql(sql , new Object[]{startupCount ,  appID , childID, time});
        }
    }

    public int saveChildMinuteDeviceInstallCount(String time, int appID, int childID, long newDeviceCount, long startupCount) {
        String tableName = "stat_child_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL, new Object[]{appID, childID, time});
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id ,child_id , new_device_count , startup_device_count , time) values (?,?,?,?,?)";
            return executeSql(sql , new Object[]{appID, childID, newDeviceCount, startupCount, time});
        }else{
            String sql = "update " + tableName + " set new_device_count = ? , startup_device_count = ? where app_id = ? and child_id = ? and time = ?";
            return executeSql(sql , new Object[]{newDeviceCount, startupCount, appID, childID, time});
        }
    }
}
