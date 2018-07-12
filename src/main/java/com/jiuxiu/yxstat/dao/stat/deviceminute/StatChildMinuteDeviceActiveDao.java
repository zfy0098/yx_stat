package com.jiuxiu.yxstat.dao.stat.deviceminute;

import com.jiuxiu.yxstat.db.StatDataBase;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.Serializable;
import java.util.Map;

public class StatChildMinuteDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatChildMinuteDeviceActiveDao statChildMinuteDeviceActiveDao  = new StatChildMinuteDeviceActiveDao();

    private StatChildMinuteDeviceActiveDao() {
    }

    public synchronized static StatChildMinuteDeviceActiveDao getInstance() {
        return statChildMinuteDeviceActiveDao;
    }


    Object readResolve(){
        return statChildMinuteDeviceActiveDao;
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
