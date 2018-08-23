package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class StatUserRegisterLoginPackageIDDao extends StatDataBase implements Serializable {


    private static StatUserRegisterLoginPackageIDDao userLoginPackageIDDao = new StatUserRegisterLoginPackageIDDao();

    private StatUserRegisterLoginPackageIDDao() {
    }

    public static StatUserRegisterLoginPackageIDDao getInstance() {
        return userLoginPackageIDDao;
    }

    Object readResolve() {
        return userLoginPackageIDDao;
    }


    /**
     * 保存package id 用户注册登录数
     *
     * @param date                          日期
     * @param appID                         应用id
     * @param childID                       马甲包id
     * @param channelID                     通道id
     * @param appChannelID                  子渠道id
     * @param packageID                     包id
     * @param packageIDLoginCount           登录数
     * @param packageIDGuestRegisterCount   游客注册数
     * @param packageIDAccountRegisterCount 账号注册数
     * @param packageIDPhoneRegisterCount   手机号注册数
     * @param packageIDQQRegisterCount      qq注册数
     * @param packageIDWXRegisterCount      微信注册数
     * @param packageIDTotalRegisterCount   注册总数
     * @return
     */
    public int savePackageIDRegisterLoginCount(String date, int appID, int childID, int channelID, int appChannelID, int packageID, long packageIDLoginCount, long packageIDGuestRegisterCount, long packageIDAccountRegisterCount,
                                               long packageIDPhoneRegisterCount, long packageIDQQRegisterCount, long packageIDWXRegisterCount, long packageIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_package_id_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, package_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count," +
                    " wx_register_count, total_register_count, date) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?) ";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, appChannelID, packageID, packageIDLoginCount, packageIDGuestRegisterCount, packageIDAccountRegisterCount,
                    packageIDPhoneRegisterCount, packageIDQQRegisterCount, packageIDWXRegisterCount, packageIDTotalRegisterCount, date});
        } else {
            String sql = "update " + tableName + " set login_count = ? , guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count=?" +
                    " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            return executeSql(sql, new Object[]{packageIDLoginCount, packageIDGuestRegisterCount, packageIDAccountRegisterCount, packageIDPhoneRegisterCount, packageIDQQRegisterCount,
                    packageIDWXRegisterCount, packageIDTotalRegisterCount, appID, childID, channelID, appChannelID, packageID, date});
        }
    }


    /**
     * 保存package id 用户分钟注册登录数
     *
     * @param time                          时间
     * @param appID                         应用id
     * @param childID                       马甲包id
     * @param channelID                     通道id
     * @param appChannelID                  子渠道id
     * @param packageID                     包id
     * @param packageIDLoginCount           登录数
     * @param packageIDGuestRegisterCount   游客注册数
     * @param packageIDAccountRegisterCount 账号注册数
     * @param packageIDPhoneRegisterCount   手机号注册数
     * @param packageIDQQRegisterCount      qq注册数
     * @param packageIDWXRegisterCount      微信注册数
     * @param packageIDTotalRegisterCount   注册总数
     * @return
     */
    public int savePackageIDMinuteRegisterLoginCount(String time, int appID, int childID, int channelID, int appChannelID, int packageID, long packageIDLoginCount, long packageIDGuestRegisterCount, long packageIDAccountRegisterCount,
                                                     long packageIDPhoneRegisterCount, long packageIDQQRegisterCount, long packageIDWXRegisterCount, long packageIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_package_id_minute_" + time.substring(0, 7).replace("-", "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, packageID, time});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, package_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count," +
                    " wx_register_count, total_register_count, time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? ,?) ";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, appChannelID, packageID, packageIDLoginCount, packageIDGuestRegisterCount, packageIDAccountRegisterCount,
                    packageIDPhoneRegisterCount, packageIDQQRegisterCount, packageIDWXRegisterCount, packageIDTotalRegisterCount, time});
        } else {
            String sql = "update " + tableName + " set login_count = ? , guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count=?" +
                    " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and time = ?";
            return executeSql(sql, new Object[]{packageIDLoginCount, packageIDGuestRegisterCount, packageIDAccountRegisterCount, packageIDPhoneRegisterCount, packageIDQQRegisterCount,
                    packageIDWXRegisterCount, packageIDTotalRegisterCount, appID, childID, channelID, appChannelID, packageID, time});
        }
    }
}
