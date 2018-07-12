package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatAppChannelIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = new StatAppChannelIdDeviceActiveDao();

    private StatAppChannelIdDeviceActiveDao() {}

    public synchronized static StatAppChannelIdDeviceActiveDao getInstance(){
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
    public int saveAppChannelIdStartUpCount(String date, String appid, String childID, String channelID, String appChannelID, int startupCount) {

        String tableName = "stat_app_channel_id_device_active_" + date.substring(0 , 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
        Map<String,Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, date });
        if(map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, startup_count, date) values (?,?,?,?,?,?)";
            return  executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, startupCount, date});
        }else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ? ";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, appChannelID, date});
        }
    }

    /**
     *    保存 app channel id 设备新增和设备启动
     * @return
     */
    public int saveAppChannelIdDeviceInstallCount(String date, int appID, int childID, int channelID, int appChannelID, long newDeviceCount, long startupCount){

        String tableName = "stat_app_channel_id_device_active_" + date.substring(0 , 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{appID, childID, channelID, appChannelID, date});
        if (map == null || map.isEmpty()){
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id ,app_channel_id, new_device_count, startup_device_count, date) values (?,?,?,?,?,?,?)";
            return executeSql(sql , new Object[]{appID, childID, channelID, appChannelID, newDeviceCount, startupCount, date});
        } else {
            String sql = "update " + tableName + " set new_device_count=?, startup_device_count=? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ? ";
            return executeSql(sql , new Object[]{newDeviceCount,startupCount,appID,childID,channelID,appChannelID,date});
        }
    }
}
