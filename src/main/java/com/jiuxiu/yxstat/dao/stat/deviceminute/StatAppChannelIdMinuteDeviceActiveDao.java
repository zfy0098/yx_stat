package com.jiuxiu.yxstat.dao.stat.deviceminute;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatAppChannelIdMinuteDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatAppChannelIdMinuteDeviceActiveDao statAppChannelIdDeviceActiveDao  = new StatAppChannelIdMinuteDeviceActiveDao();

    private StatAppChannelIdMinuteDeviceActiveDao() {}

    public synchronized static StatAppChannelIdMinuteDeviceActiveDao getInstance(){
        return statAppChannelIdDeviceActiveDao;
    }


    Object readResolve(){
        return statAppChannelIdDeviceActiveDao;
    }

    /**
     * 保存 app channel id启动数
     *
     * @param
     * @return
     */
    public int saveAppChannelIDMinuteStartUpCount(String time, String appid, String childID, String channelID, String appChannelID, int startupCount) {

        String tableName = "stat_app_channel_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, time });
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, startup_count, time) values (?,?,?,?,?,?)";
            return  executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, startupCount, time});
        }else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and time = ? ";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, appChannelID , time});
        }
    }

    public int saveAppChannelIDMinuteDeviceInstallCount(String time, int appID, int childID, int channelID, int appChannelID, long newDeviceCount, long startupCount){

        String tableName = "stat_app_channel_id_device_active_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and time = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{appID, childID, channelID, appChannelID, time});
        if (map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id ,app_channel_id , new_device_count,startup_device_count, time) values (?,?,?,?,?,?,?)";
            return executeSql(sql , new Object[]{appID, childID, channelID, appChannelID, newDeviceCount, startupCount, time});
        } else {
            String sql = "update " + tableName + " set new_device_count=? , startup_device_count=? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and time = ? ";
            return executeSql(sql , new Object[]{newDeviceCount,startupCount,appID,childID,channelID,appChannelID,time});
        }
    }
}
