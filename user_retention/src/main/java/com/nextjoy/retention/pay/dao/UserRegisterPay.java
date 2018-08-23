package com.nextjoy.retention.pay.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;

/**
 * Created with IDEA by hadoop on 2018/8/17.
 *
 * @author Choufy
 */
public class UserRegisterPay extends StatDataBase implements Serializable {

    private static UserRegisterPay userRegisterPay = new UserRegisterPay();

    private UserRegisterPay (){}

    public static UserRegisterPay getInstance(){
        return userRegisterPay;
    }

    Object readResolve(){
        return userRegisterPay;
    }


    public void saveAppIDRegisterPay(int appid , long count, long money, String date){
        String sql = "insert into stat_user_pay_retention_rate_app_id (app_id, register_count, money, register_date) values (?,?,?,?)";
        executeSql(sql , new Object[]{appid , count, money, date});
    }

    public void saveChildIDRegisterPay(int appid, int childID, long count, long money, String date){
        String sql = "insert into stat_user_pay_retention_rate_child_id (app_id, child_id, register_count, money, register_date) values (?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, count, money, date});
    }

    public void saveChannelIDRegisterPay(int appid, int childID, int channelID, long count, long money, String date){
        String sql = "insert into stat_user_pay_retention_rate_channel_id (app_id, child_id, channel_id, register_count, money, register_date) values (?,?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, count, money, date});
    }

    public void saveAppChannelIDRegisterPay(int appid, int childID, int channelID, int appChannelID, long count, long money, String date){
        String sql = "insert into stat_user_pay_retention_rate_app_channel_id (app_id, child_id, channel_id, app_channel_id, register_count, money, register_date) values (?,?,?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, count, money, date});
    }

    public void savePackageIDRegisterPay(int appid, int childID, int channelID, int appChannelID, int packageID, long count, long money, String date){
        String sql = "insert into stat_user_pay_retention_rate_package_id (app_id, child_id, channel_id, app_channel_id, package_id,register_count, money, register_date) values (?,?,?,?,?,?,?,?)";
        executeSql(sql , new Object[]{appid, childID, channelID, appChannelID, packageID, count, money, date});
    }


}
