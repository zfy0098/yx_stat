package com.nextjoy.retention.device.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;


public class DeviceRetentionDao extends StatDataBase implements Serializable {


    private static DeviceRetentionDao deviceRetentionDao = null;

    private DeviceRetentionDao() {
    }

    public static DeviceRetentionDao getInstance() {
        if (deviceRetentionDao == null) {
            deviceRetentionDao = new DeviceRetentionDao();
        }
        return deviceRetentionDao;
    }

    Object readResolve() {
        return deviceRetentionDao;
    }


    public void saveAppIDDeviceRetention(int appid, long count, String date, String col) {
        String sql = "select id from stat_device_retention_app_id where app_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, date});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_device_retention_app_id (app_id, " + col + ", active_date) values (?,?,?)";
            executeSql(sql, new Object[]{appid, count, date});
        } else {
            sql = "update stat_device_retention_app_id set " + col + " = ? where app_id =? and active_date = ?";
            executeSql(sql, new Object[]{count, appid, date});
        }
    }


    public void saveChildIDDeviceRetention(int appid, int childID, long count, String date, String col) {
        String sql = "select id from stat_device_retention_child_id where app_id = ? and child_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_device_retention_child_id (app_id, child_id, " + col + " , active_date) values (?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, count, date});
        } else {
            sql = "update stat_device_retention_child_id set " + col + " = ? where app_id = ? and child_id = ? and active_date = ?";
            executeSql(sql, new Object[]{count, appid, childID, date});
        }
    }


    public void saveChannelIDDeviceRetention(int appid, int childID, int channelID, long count, String date, String col) {
        String sql = "select id from stat_device_retention_channel_id where app_id = ? and child_id=? and channel_id =? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_device_retention_channel_id (app_id, child_id, channel_id, " + col + " , active_date) values (?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, count, date});
        } else {
            sql = "update stat_device_retention_channel_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and active_date = ?";
            executeSql(sql, new Object[]{count, appid, childID, channelID, date});
        }
    }


    public void saveAppChannelIDDeviceRetention(int appid, int childID, int channelID, int appChannelID, long count, String date, String col) {
        String sql = "select id from stat_device_retention_app_channel_id where app_id = ? and child_id=? and channel_id =? and app_channel_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_device_retention_app_channel_id (app_id, child_id, channel_id, app_channel_id, " + col + " , active_date) values (?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, count, date});
        } else {
            sql = "update stat_device_retention_app_channel_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and active_date = ?";

            executeSql(sql, new Object[]{count, appid, childID, channelID, appChannelID, date});
        }

    }


    public void savePackageIDDeviceRetention(int appid, int childID, int channelID, int appChannelID, int packageID, long count, String date, String col) {
        String sql = "select id from stat_device_retention_package_id where app_id = ? and child_id=? and channel_id =? and app_channel_id = ? and package_id = ? and active_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_device_retention_package_id (app_id, child_id, channel_id, app_channel_id, package_id, " + col + " , active_date) values (?,?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, count, date});
        } else {
            sql = "update stat_device_retention_package_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and active_date = ?";
            executeSql(sql, new Object[]{count, appid, childID, channelID, appChannelID, packageID, date});
        }

    }


}
