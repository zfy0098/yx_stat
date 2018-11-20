package com.nextjoy.retention.device.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

public class DeviceActivateDao extends StatDataBase implements Serializable {


    private static DeviceActivateDao deviceActivateDao = null;

    private DeviceActivateDao() {
    }

    public static DeviceActivateDao getInstance() {
        if (deviceActivateDao == null) {
            deviceActivateDao = new DeviceActivateDao();
        }
        return deviceActivateDao;
    }

    public void saveAppidDeviceActivate(int appid, long count, String date) {
        String sql = "select id from stat_device_retention_app_id where app_id = ?  and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, date});
        if (map == null || map.isEmpty()) {
            String saveSql = "insert into stat_device_retention_app_id (app_id, active_count, active_date) values (?,?,?)";
            executeSql(saveSql, new Object[]{appid, count, date});
        } else {
            String update = "update stat_device_retention_app_id set active_count = ? where app_id = ?  and active_date = ?";
            executeSql(update, new Object[]{count, appid, date});
        }
    }

    public void saveChildIDDeviceActivate(int appid, int childID, long count, String date) {
        String sql = "select id from stat_device_retention_child_id where app_id = ? and child_id =? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, date});
        if (map == null || map.isEmpty()) {
            String saveSql = "insert into stat_device_retention_child_id (app_id, child_id, active_count, active_date) values (?,?,?,?)";
            executeSql(saveSql, new Object[]{appid, childID, count, date});
        } else {
            String update = "update stat_device_retention_child_id set active_count = ? where app_id = ? and child_id = ?  and active_date = ?";
            executeSql(update, new Object[]{count, appid, childID, date});
        }
    }

    public void saveChannelIDDeviceActivate(int appid, int childID, int channelID, long count, String date) {
        String sql = "select id from stat_device_retention_channel_id where app_id = ? and child_id = ? and channel_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            String saveSql = "insert into stat_device_retention_channel_id (app_id, child_id, channel_id, active_count, active_date) values (?,?,?,?,?)";
            executeSql(saveSql, new Object[]{appid, childID, channelID, count, date});
        } else {
            String update = "update stat_device_retention_channel_id set active_count = ? where app_id = ? and child_id = ? and channel_id = ? and active_date = ?";
            executeSql(update, new Object[]{count, appid, childID, channelID, date});
        }
    }


    public void saveAppChannelIDDeviceActivate(int appid, int childId, int channelID, int appChannelID, long count, String date) {
        String sql = "select id from stat_device_retention_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childId, channelID, appChannelID, date});
        if (map == null || map.isEmpty()) {
            String saveSql = "insert into stat_device_retention_app_channel_id (app_id, child_id, channel_id, app_channel_id, active_count, active_date) values (?,?,?,?,?,?)";
            executeSql(saveSql, new Object[]{appid, childId, channelID, appChannelID, count, date});
        } else {
            String update = "update stat_device_retention_app_channel_id set active_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and active_date = ?";
            executeSql(update, new Object[]{count, appid, childId, channelID, appChannelID, date});
        }
    }


    public void savePackageIDDeviceActivate(int appid, int childID, int channelId, int appChannelID, int packageID, long count, String date) {
        String sql = "select id from stat_device_retention_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and active_date = ?";

        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelId, appChannelID, packageID, date});

        if (map == null || map.isEmpty()) {
            String saveSql = "insert into stat_device_retention_package_id (app_id, child_id, channel_id, app_channel_id, package_id, active_count, active_date) values (?,?,?,?,?,?,?)";
            executeSql(saveSql, new Object[]{appid, childID, channelId, appChannelID, packageID, count, date});
        } else {
            String update = "update stat_device_retention_package_id set active_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and active_date = ?";
            executeSql(update, new Object[]{count, appid, childID, channelId, appChannelID, packageID, date});
        }
    }
}
