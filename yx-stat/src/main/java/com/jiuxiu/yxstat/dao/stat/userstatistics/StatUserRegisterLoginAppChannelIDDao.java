package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class StatUserRegisterLoginAppChannelIDDao extends StatDataBase implements Serializable {


    private static StatUserRegisterLoginAppChannelIDDao statUserLoginAppChannelIDDao = new StatUserRegisterLoginAppChannelIDDao();

    private StatUserRegisterLoginAppChannelIDDao() {
    }

    public static StatUserRegisterLoginAppChannelIDDao getInstance() {
        return statUserLoginAppChannelIDDao;
    }

    Object readResolve() {
        return statUserLoginAppChannelIDDao;
    }


    /**
     *  保存 app channel id 用户注册登录数
     * @param date     日期
     * @param appID                    应用id
     * @param childID                  马甲包id
     * @param channelID                渠道id
     * @param appChannelID             子渠道id
     * @param appChannelIDLoginCount   登录数
     * @param appChannelIDGuestRegisterCount   游客注册数
     * @param appChannelIDAccountRegisterCount 账号注册数
     * @param appChannelIDPhoneRegisterCount   手机号注册数
     * @param appChannelIDQQRegisterCount      QQ注册数
     * @param appChannelIDWXRegisterCount      微信注册数
     * @param appChannelIDTotalRegisterCount   注册总数
     * @return   数据库影响行数
     */
    public int saveAppChannelIDRegisterLoginCount(String date, int appID, int childID, int channelID, int appChannelID, long appChannelIDLoginCount, long appChannelIDGuestRegisterCount,
                                                  long appChannelIDAccountRegisterCount, long appChannelIDPhoneRegisterCount, long appChannelIDQQRegisterCount, long appChannelIDWXRegisterCount,
                                                  long appChannelIDOtherRegisterCount, long appChannelIDTotalRegisterCount) {
        String tableName = "stat_user_register_login_app_channel_id_" + date.substring(0,4);

        String querySQL = "select id from " + tableName +" where app_id = ? and child_id = ? and channel_id = ? and  app_channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, date});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, total_register_count, date) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, appChannelID, appChannelIDLoginCount, appChannelIDGuestRegisterCount, appChannelIDAccountRegisterCount,
                    appChannelIDPhoneRegisterCount, appChannelIDQQRegisterCount, appChannelIDWXRegisterCount, appChannelIDOtherRegisterCount, appChannelIDTotalRegisterCount, date});
        } else {
            String sql = "update " + tableName + " set login_count = ?, guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ?, other_register_count = ?, total_register_count=?" +
                    " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
            return executeSql(sql, new Object[]{appChannelIDLoginCount, appChannelIDGuestRegisterCount, appChannelIDAccountRegisterCount, appChannelIDPhoneRegisterCount,
                    appChannelIDQQRegisterCount, appChannelIDWXRegisterCount, appChannelIDOtherRegisterCount, appChannelIDTotalRegisterCount, appID, childID, channelID, appChannelID, date});
        }
    }

    /**
     *    保存 app channel id 分钟 用户注册登录数
     * @param time                   时间
     * @param appID                  应用id
     * @param childID                马甲包id
     * @param channelID              渠道id
     * @param appChannelID           子渠道id
     * @param appChannelIDLoginCount   登录数
     * @param appChannelIDGuestRegisterCount   游客注册数
     * @param appChannelIDAccountRegisterCount 账号注册数
     * @param appChannelIDPhoneRegisterCount   手机号注册数
     * @param appChannelIDQQRegisterCount      QQ注册数
     * @param appChannelIDWXRegisterCount      微信注册数
     * @param appChannelIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveAppChannelIDMinuteRegisterLoginCount(String time, int appID, int childID, int channelID, int appChannelID, long appChannelIDLoginCount, long appChannelIDGuestRegisterCount,
                                                        long appChannelIDAccountRegisterCount, long appChannelIDPhoneRegisterCount, long appChannelIDQQRegisterCount, long appChannelIDWXRegisterCount,
                                                        long appChannelIDOtherRegisterCount, long appChannelIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_app_channel_id_minute_" + time.substring(0 , 7).replace("-" , "");

        String querySQL = "select id from " + tableName + " where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and time = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, appChannelID, time});
        if (map == null || map.isEmpty()) {
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, app_channel_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, total_register_count, time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, appChannelID, appChannelIDLoginCount, appChannelIDGuestRegisterCount, appChannelIDAccountRegisterCount,
                    appChannelIDPhoneRegisterCount, appChannelIDQQRegisterCount, appChannelIDWXRegisterCount, appChannelIDOtherRegisterCount, appChannelIDTotalRegisterCount, time});

        } else {
            String sql = "update " + tableName + " set login_count = ?, guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ?, other_register_count = ?, total_register_count=?" +
                    " where  app_id = ? and child_id = ? and channel_id = ? and  app_channel_id = ? and time = ?";
            return executeSql(sql, new Object[]{appChannelIDLoginCount, appChannelIDGuestRegisterCount, appChannelIDAccountRegisterCount, appChannelIDPhoneRegisterCount,
                    appChannelIDQQRegisterCount, appChannelIDWXRegisterCount, appChannelIDOtherRegisterCount, appChannelIDTotalRegisterCount, appID, childID, channelID, appChannelID, time});
        }
    }
}
