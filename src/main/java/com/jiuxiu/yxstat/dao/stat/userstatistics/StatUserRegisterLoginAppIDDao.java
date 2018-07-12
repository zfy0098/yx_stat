package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class StatUserRegisterLoginAppIDDao extends StatDataBase implements Serializable {

    private static StatUserRegisterLoginAppIDDao statUserRegisterLoginAppIDDao = new StatUserRegisterLoginAppIDDao();

    private StatUserRegisterLoginAppIDDao() {
    }

    public static StatUserRegisterLoginAppIDDao getInstance() {
        return statUserRegisterLoginAppIDDao;
    }

    Object readResolve() {
        return statUserRegisterLoginAppIDDao;
    }


    /**
     * 保存 app id 注册登录数
     *
     * @param date                      日期
     * @param appID                     应用id
     * @param appIDLoginCount           app 登录数
     * @param appIDGuestRegisterCount   游客注册数
     * @param appIDAccountRegisterCount 账号户注册数
     * @param appIDPhoneRegisterCount   手机号注册数
     * @param appIDQQRegisterCount      QQ注册数
     * @param appIDWXRegisterCount      微信注册数
     * @param appIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveAppIDRegisterLoginCount(String date, int appID, long appIDLoginCount, long appIDGuestRegisterCount, long appIDAccountRegisterCount, long appIDPhoneRegisterCount,
                                           long appIDQQRegisterCount, long appIDWXRegisterCount, long appIDTotalRegisterCount) {
        String querySql = "select id from stat_user_register_login_app_id where app_id =? and date = ?";
        Map<String, Object> map = queryForMap(querySql, new Object[]{appID, date});
        if (map == null || map.isEmpty()) {
            // 没有数据 插入一条
            String insertSQL = "insert into stat_user_register_login_app_id (app_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count," +
                    " wx_register_count, total_register_count, date) values (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
            return executeSql(insertSQL, new Object[]{appID, appIDLoginCount, appIDGuestRegisterCount, appIDAccountRegisterCount, appIDPhoneRegisterCount, appIDQQRegisterCount, appIDWXRegisterCount,
                    appIDTotalRegisterCount, date});
        } else {
            //  存在数据  更新操作
            String sql = "update stat_user_register_login_app_id set login_count = ?, guest_register_count = ?, account_register_count = ?," +
                    " phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count = ? where app_id = ? and date = ? ";
            return executeSql(sql, new Object[]{appIDLoginCount, appIDGuestRegisterCount, appIDAccountRegisterCount, appIDPhoneRegisterCount, appIDQQRegisterCount, appIDWXRegisterCount,
                    appIDTotalRegisterCount, appID, date});
        }
    }

    /**
     * 保存 app id 分钟注册登录数
     *
     * @param time                      时间
     * @param appID                     应用id
     * @param appIDLoginCount           app 登录数
     * @param appIDGuestRegisterCount   游客注册数
     * @param appIDAccountRegisterCount 账号户注册数
     * @param appIDPhoneRegisterCount   手机号注册数
     * @param appIDQQRegisterCount      QQ注册数
     * @param appIDWXRegisterCount      微信注册数
     * @param appIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveAppIDMinuteRegisterLoginCount(String time, int appID, long appIDLoginCount, long appIDGuestRegisterCount, long appIDAccountRegisterCount, long appIDPhoneRegisterCount,
                                                 long appIDQQRegisterCount, long appIDWXRegisterCount, long appIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_app_id_minute_" + time.substring(0, 7).replace("-", "");

        String querySql = "select id from " + tableName + " where app_id =? and time = ?";
        Map<String, Object> map = queryForMap(querySql, new Object[]{appID, time});
        if (map == null || map.isEmpty()) {
            // 没有数据 插入一条
            String insertSQL = "insert into " + tableName + " (app_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count," +
                    " wx_register_count, total_register_count, time) values (?, ?, ?, ?, ?, ?, ?, ?, ?) ";
            return executeSql(insertSQL, new Object[]{appID, appIDLoginCount, appIDGuestRegisterCount, appIDAccountRegisterCount, appIDPhoneRegisterCount, appIDQQRegisterCount, appIDWXRegisterCount,
                    appIDTotalRegisterCount, time});
        } else {
            //  存在数据  更新操作
            String sql = "update " + tableName + " set login_count = ?, guest_register_count = ?, account_register_count = ?," +
                    " phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , total_register_count = ? where app_id = ? and time = ? ";
            return executeSql(sql, new Object[]{appIDLoginCount, appIDGuestRegisterCount, appIDAccountRegisterCount, appIDPhoneRegisterCount, appIDQQRegisterCount, appIDWXRegisterCount,
                    appIDTotalRegisterCount, appID, time});
        }
    }
}
