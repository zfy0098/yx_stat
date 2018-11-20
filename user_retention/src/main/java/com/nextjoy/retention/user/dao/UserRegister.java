package com.nextjoy.retention.user.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * @author hadoop
 */
public class UserRegister extends StatDataBase implements Serializable {


    private static UserRegister  userRegister = new UserRegister();

    private UserRegister(){}

    public static UserRegister getInstance(){
        return userRegister;
    }

    Object readResolve(){
        return userRegister;
    }


    public void saveAppIDRegister(int appid , long count, String date){
        String sql = "select id from stat_user_retention_rate_app_id where app_id = ? and register_date = ?";
        Map<String,Object> map = queryForMap(sql, new Object[]{appid, date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_app_id (app_id, register_count, register_date) values (?,?,?)";
            executeSql(sql , new Object[]{appid, count, date});
        } else {
            sql = "update stat_user_retention_rate_app_id set register_count = ? where  app_id = ? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, date});
        }
    }

    public void saveChildIDRegister(int appid, int childID, long count, String date){
        String sql = "select id from stat_user_retention_rate_child_id where app_id = ? and child_id = ? and register_date = ?";
        Map<String,Object> map = queryForMap(sql , new Object[]{appid, childID, date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_child_id (app_id, child_id, register_count, register_date) values (?,?,?,?)";
            executeSql(sql , new Object[]{appid, childID, count, date});
        } else {
            sql = "update stat_user_retention_rate_child_id set register_count = ? where app_id = ? and child_id = ? and register_date = ? ";
            executeSql(sql, new Object[]{count, appid, childID, date});
        }
    }

    public void saveChannelIDRegister(int appid, int childID, int channelID, long count, String date){
        String sql = "select id from stat_user_retention_rate_channel_id where app_id = ? and child_id = ? and channel_id = ? and register_date = ? ";
        Map<String,Object> map = queryForMap(sql , new Object[]{appid, childID, channelID, date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_channel_id (app_id, child_id, channel_id, register_count, register_date) values (?,?,?,?,?)";
            executeSql(sql , new Object[]{appid, childID, channelID, count, date});
        } else {
            sql = "update stat_user_retention_rate_channel_id set register_count = ? where app_id = ? and child_id = ? and channel_id = ? and register_date = ?";
            executeSql(sql, new Object[]{count, appid, childID, channelID, date});
        }
    }

    public void saveAppChannelIDRegister(int appid, int childID, int channelID, int appChannelID, long count, String date){

        String sql = "select id from stat_user_retention_rate_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and register_date = ? ";
        Map<String,Object> map = queryForMap(sql , new Object[]{appid, childID, channelID, appChannelID, date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_app_channel_id (app_id, child_id, channel_id, app_channel_id, register_count, register_date) values (?,?,?,?,?,?)";
            executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, count, date});
        } else {
           sql = "update stat_user_retention_rate_app_channel_id set register_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and register_date = ?" ;
           executeSql(sql, new Object[]{count, appid, childID, channelID, appChannelID, date});
        }
    }

    public void savePackageIDRegister(int appid, int childID, int channelID, int appChannelID, int packageID, long count, String date){
        String sql = "select id from stat_user_retention_rate_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and register_date = ? ";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
        if(map == null || map.isEmpty()){
            sql = "insert into stat_user_retention_rate_package_id (app_id, child_id, channel_id, app_channel_id, package_id,register_count, register_date) values (?,?,?,?,?,?,?)";
            executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, packageID, count, date});
        } else {
            sql = "update stat_user_retention_rate_package_id set register_count = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and register_date = ? ";
            executeSql(sql, new Object[]{count, appid, childID, channelID, appChannelID, packageID, date});
        }
    }
}
