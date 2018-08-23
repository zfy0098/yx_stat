package com.nextjoy.retention.user.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * @author hadoop
 */
public class UserRetention extends StatDataBase implements Serializable {

    private static UserRetention userRetention = new UserRetention();

    private UserRetention() { }

    public static UserRetention getInstance() {
        return userRetention;
    }

    Object readResolve() {
        return userRetention;
    }


    /**
     * 保存 app 留存情况
     *
     * @param appid appid
     * @param count 留存数量
     * @param date  注册日期
     * @param col   留存对应的列名 次日留存列名为 day2
     */
    public void saveAppIDRetention(int appid, long count, String date, String col) {
        String sql = "select id from stat_user_retention_rate_app_id where app_id = ? and  register_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_user_retention_rate_app_id (app_id , " + col + " , register_date) values (?,?,?)";
            executeSql(sql, new Object[]{appid, count, date});
        } else {
            sql = "update stat_user_retention_rate_app_id set " + col + " = ?  where app_id = ? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, date});
        }
    }

    public void saveChildIDRetention(int appid , int childid ,  long count, String date, String col){
        String sql = "select id  from stat_user_retention_rate_child_id where app_id = ? and child_id = ? and register_date = ?";
        Map<String,Object> map = queryForMap(sql , new Object[]{appid , childid , date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_child_id (app_id , child_id, " + col + " , register_date )  values (?,?,?, ?) ";
            executeSql(sql , new Object[]{appid , childid , count , date});
        } else {
            sql = "update stat_user_retention_rate_child_id set " + col + " = ? where app_id = ? and child_id = ? and register_date = ? ";
            executeSql(sql, new Object[]{count, appid, childid, date});
        }
    }

    public void saveChannelIDRetention(int appid, int childid, int channelID, long count, String date, String col) {
        String sql = "select id  from stat_user_retention_rate_channel_id where app_id = ? and child_id = ? and channel_id = ?  and register_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childid, channelID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_user_retention_rate_channel_id (app_id, child_id, channel_id, " + col + " , register_date) values (?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childid, channelID, count, date});
        } else {
            sql = "update stat_user_retention_rate_channel_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, childid, channelID, date});
        }
    }


    public void saveAppChannelIDRetention(int appid, int childid, int channelID, int appChannelID, long count, String date, String col) {
        String sql = "select id  from stat_user_retention_rate_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and register_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childid, channelID,appChannelID, date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_user_retention_rate_app_channel_id (app_id, child_id, channel_id, app_channel_id, " + col + " , register_date) values (?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childid,channelID , appChannelID, count, date});
        } else {
            sql = "update stat_user_retention_rate_app_channel_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id= ? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, childid, channelID, appChannelID , date});
        }
    }

    public void savePackageIDRetention(int appid, int childid, int channelID, int appChannelID, int packageID, long count, String date, String col) {
        String sql = "select id  from stat_user_retention_rate_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and register_date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childid, channelID, appChannelID, packageID ,  date});
        if (map == null || map.isEmpty()) {
            sql = "insert into stat_user_retention_rate_package_id (app_id, child_id, channel_id, app_channel_id, package_id, " + col + " , register_date) values (?,?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childid,channelID , appChannelID, packageID, count, date});
        } else {
            sql = "update stat_user_retention_rate_package_id set " + col + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id= ? and package_id=? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, childid, channelID, appChannelID, packageID, date});
        }
    }

}
