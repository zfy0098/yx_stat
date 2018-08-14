package com.jiuxiu.yxstat.dao.stat.deviceinstall;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class StatPackageIdDeviceActiveDao extends StatDataBase implements Serializable {

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = new StatPackageIdDeviceActiveDao();

    private StatPackageIdDeviceActiveDao() {
    }

    public synchronized static StatPackageIdDeviceActiveDao getInstance() {
        return statPackageIdDeviceActiveDao;
    }

    Object readResolve() {
        return statPackageIdDeviceActiveDao;
    }

    /**
     * 保存 package id 启动数
     *
     * @return
     */
    public int savePackageIdStartUpCount(String date, String appid, String childID, String channelID, String appChannelID, String packageID, int startupCount) {

        String tableName = "stat_package_id_device_active_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, package_id, startup_count, date) values (?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, startupCount, date});
        } else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, appChannelID, packageID, date});
        }
    }

    /**
     * 保存 package id 设备新增 和 设备启动数
     *
     * @return
     */
    public int savePackageIdDeviceInstallCount(String date, int appID, int childID, int channelID, int appChannelID, int packageID, long newDeviceCount, long startupCount) {
        String tableName = "stat_package_id_device_active_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id , child_id , channel_id , app_channel_id, package_id , new_device_count,startup_device_count, date) values (?,?,?,?,?,?,?,?) ";
            return executeSql(sql, new Object[]{appID, childID, channelID, appChannelID, packageID, newDeviceCount, startupCount, date});
        } else {
            String sql = "update " + tableName + " set new_device_count=? , startup_device_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            return executeSql(sql, new Object[]{newDeviceCount, startupCount, appID, childID, channelID, appChannelID, packageID, date});
        }
    }



    /**
     * 保存 package id 启动数
     *
     * @param
     * @return
     */
    public int savePackageIDMinuteStartUpCount(String time, String appid, String childID, String channelID, String appChannelID, String packageID, int startupCount) {
        String tableName = "stat_package_id_device_active_minute_" + time.substring(0, 7).replace("-", "");
        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, packageID, time});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, package_id, startup_count, time) values (?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, startupCount, time});
        } else {
            String sql = "update " + tableName + " set startup_count = ifnull(startup_count,0) + ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
            return executeSql(sql, new Object[]{startupCount, appid, childID, channelID, appChannelID, packageID, time});
        }
    }


    public int savePackageIDMinuteDeviceInstallCount(String time, int appID, int childID, int channelID, int appChannelID, int packageID, long newDeviceCount, long startupCount) {
        String tableName = "stat_package_id_device_active_minute_" + time.substring(0, 7).replace("-", "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, packageID, time});
        if (map == null || map.isEmpty()) {
            String sql = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, package_id, new_device_count, startup_device_count, time) values (?,?,?,?,?,?,?,?) ";
            return executeSql(sql, new Object[]{appID, childID, channelID, appChannelID, packageID, newDeviceCount, startupCount, time});
        } else {
            String sql = "update " + tableName + " set new_device_count = ?, startup_device_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
            return executeSql(sql, new Object[]{newDeviceCount, startupCount, appID, childID, channelID, appChannelID, packageID, time});
        }
    }

}
