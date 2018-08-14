package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatChannelIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = new StatChannelIdDeviceActiveDao();

    private StatChannelIdDeviceActiveDao() {
    }

    public synchronized static StatChannelIdDeviceActiveDao getInstance() {
        return statChannelIdDeviceActiveDao;
    }


    Object readResolve() {
        return statChannelIdDeviceActiveDao;
    }

    /**
     * 保存 channel id启动数
     *
     * @param
     * @return
     */
    public int saveChannelIdStartUpCount(String date, String appid, String childID, String channelID, int startupCount) {
        String tableName = "stat_channel_id_device_active_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, startup_count, date) values (?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, startupCount, date});
        } else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ?  where app_id = ? and child_id = ? and channel_id = ? and date = ?";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, date});
        }
    }

    /**
     * 保存 channel id 设备新增和设备启动
     *
     * @return
     */
    public int saveChannelIdDeviceInstallCount(String date, int appID, int childID, int channelID, long newDeviceCount, long startupCount) {
        String tableName = "stat_channel_id_device_active_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert  into " + tableName + " (app_id , child_id , channel_id , new_device_count,startup_device_count , date) values (?,?,?,?,?,?)";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, newDeviceCount, startupCount, date});
        } else {
            String sql = "update " + tableName + " set new_device_count=? , startup_device_count=? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and date = ?";
            return executeSql(sql, new Object[]{newDeviceCount, startupCount, appID, childID, channelID, date});
        }
    }

    /**
     * 保存 channel id  分钟启动数
     *
     * @param
     * @return
     */
    public int saveChannelIDMinuteStartUpCount(String time, String appid, String childID, String channelID, int startupCount) {
        String tableName = "stat_channel_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, time});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, startup_count, time) values (?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, startupCount, time});
        } else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and channel_id = ? and time = ?";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, time});
        }
    }


    /**
     *   保存 channel id 分钟设备信息
     * @param time
     * @param appID
     * @param childID
     * @param channelID
     * @param newDeviceCount
     * @param startupCount
     * @return
     */
    public int saveChannelIDMinuteDeviceInstallCount(String time, int appID, int childID, int channelID, long newDeviceCount, long startupCount){
        String tableName = "stat_channel_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{appID , childID , channelID , time});
        if(map == null || map.isEmpty()){
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, new_device_count, startup_device_count, time) values (?,?,?,?,?,?)";
            return executeSql(insertSQL , new Object[]{appID, childID , channelID , newDeviceCount ,startupCount , time});
        } else {
            String sql = "update " + tableName + " set new_device_count=?, startup_device_count=? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and time = ?";
            return executeSql(sql , new Object[]{newDeviceCount, startupCount, appID, childID, channelID, time});
        }
    }

}
