package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class StatUserRegisterLoginChannelIDDao extends StatDataBase implements Serializable {

    private static StatUserRegisterLoginChannelIDDao userLoginChannelIDDao = new StatUserRegisterLoginChannelIDDao();

    private StatUserRegisterLoginChannelIDDao() {
    }

    public static StatUserRegisterLoginChannelIDDao getInstance() {
        return userLoginChannelIDDao;
    }

    Object readResolve() {
        return userLoginChannelIDDao;
    }


    /**
     * 保存  channel id  用户注册登录数
     *
     * @param date                          日期
     * @param appID                         应用id
     * @param childID                       马甲包id
     * @param channelID                     通道id
     * @param channelIDLoginCount           登录数
     * @param channelIDGuestRegisterCount   游客注册数
     * @param channelIDAccountRegisterCount 账号注册数
     * @param channelIDPhoneRegisterCount   手机号注册数
     * @param channelIDQQRegisterCount      QQ注册数
     * @param channelIDWXRegisterCount      微信注册数
     * @param channelIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveChannelIDRegisterLoginCount(String date, int appID, int childID, int channelID, long channelIDLoginCount, long channelIDGuestRegisterCount, long channelIDAccountRegisterCount,
                                               long channelIDPhoneRegisterCount, long channelIDQQRegisterCount, long channelIDWXRegisterCount, long channelIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_channel_id_" + date.substring(0, 4);

        String querySQL = "select id from " + tableName + " where app_id=? and  child_id=? and channel_id=? and date=? ";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            // 没有数据
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, total_register_count, date) values (?,?,?,?,?,?,?,?,?,?,?) ";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, channelIDLoginCount, channelIDGuestRegisterCount, channelIDAccountRegisterCount,
                    channelIDPhoneRegisterCount, channelIDQQRegisterCount, channelIDWXRegisterCount, channelIDTotalRegisterCount, date});
        } else {
            String sql = "update " + tableName + " set login_count = ?, guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count=? " +
                    " where  app_id=? and  child_id=? and channel_id=? and date=? ";
            return executeSql(sql, new Object[]{channelIDLoginCount, channelIDGuestRegisterCount, channelIDAccountRegisterCount, channelIDPhoneRegisterCount,
                    channelIDQQRegisterCount, channelIDWXRegisterCount, channelIDTotalRegisterCount, appID, childID, channelID, date});
        }
    }

    /**
     * 保存  channel id  用户 分钟 注册登录数
     * @param time                          时间
     * @param appID                         应用id
     * @param childID                       马甲包id
     * @param channelID                     通道id
     * @param channelIDLoginCount           登录数
     * @param channelIDGuestRegisterCount   游客注册数
     * @param channelIDAccountRegisterCount 账号注册数
     * @param channelIDPhoneRegisterCount   手机号注册数
     * @param channelIDQQRegisterCount      QQ注册数
     * @param channelIDWXRegisterCount      微信注册数
     * @param channelIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveChannelIDMinuteRegisterLoginCount(String time, int appID, int childID, int channelID, long channelIDLoginCount, long channelIDGuestRegisterCount, long channelIDAccountRegisterCount,
                                                     long channelIDPhoneRegisterCount, long channelIDQQRegisterCount, long channelIDWXRegisterCount, long channelIDTotalRegisterCount) {
        String tableName = "stat_user_register_login_channel_id_minute_" + time.substring(0, 7).replace("-", "");

        String querySQL = "select id from " + tableName + " where app_id=? and  child_id=? and channel_id=? and time=? ";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, channelID, time});
        if (map == null || map.isEmpty()) {
            // 没有数据
            String insertSQL = "insert into " + tableName + " (app_id, child_id, channel_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, total_register_count, time) values (?,?,?,?,?,?,?,?,?,?,?) ";
            return executeSql(insertSQL, new Object[]{appID, childID, channelID, channelIDLoginCount, channelIDGuestRegisterCount, channelIDAccountRegisterCount,
                    channelIDPhoneRegisterCount, channelIDQQRegisterCount, channelIDWXRegisterCount, channelIDTotalRegisterCount, time});
        } else {
            String sql = "update " + tableName + " set login_count = ?, guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count=? " +
                    " where  app_id=? and  child_id=? and channel_id=? and time=? ";
            return executeSql(sql, new Object[]{channelIDLoginCount, channelIDGuestRegisterCount, channelIDAccountRegisterCount, channelIDPhoneRegisterCount,
                    channelIDQQRegisterCount, channelIDWXRegisterCount, channelIDTotalRegisterCount, appID, childID, channelID, time});
        }
    }
}
