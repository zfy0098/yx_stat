package com.nextjoy.retention.user.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;

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
        String sql = "insert into stat_user_retention_rate_app_id (app_id, register_count, register_date) values (?,?,?)";
        executeSql(sql , new Object[]{appid , count, date});
    }

    public void saveChildIDRegister(int appid, int childID, long count, String date){
        String sql = "insert into stat_user_retention_rate_child_id (app_id, child_id, register_count, register_date) values (?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, count, date});
    }

    public void saveChannelIDRegister(int appid, int childID, int channelID, long count, String date){
        String sql = "insert into stat_user_retention_rate_channel_id (app_id, child_id, channel_id, register_count, register_date) values (?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, count, date});
    }

    public void saveAppChannelIDRegister(int appid, int childID, int channelID, int appChannelID, long count, String date){
        String sql = "insert into stat_user_retention_rate_app_channel_id (app_id, child_id, channel_id, app_channel_id, register_count, register_date) values (?,?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, count, date});
    }

    public void savePackageIDRegister(int appid, int childID, int channelID, int appChannelID, int packageID, long count, String date){
        String sql = "insert into stat_user_retention_rate_package_id (app_id, child_id, channel_id, app_channel_id, package_id,register_count, register_date) values (?,?,?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, packageID, count, date});
    }


}
