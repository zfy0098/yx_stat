package com.jiuxiu.yxstat.dao.stat.userstatistics;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class StatUserRegisterLoginChildIDDao extends StatDataBase implements Serializable {


    private static StatUserRegisterLoginChildIDDao statUserLoginChildIDDao = new StatUserRegisterLoginChildIDDao();

    private StatUserRegisterLoginChildIDDao() {
    }

    public static StatUserRegisterLoginChildIDDao getInstance() {
        return statUserLoginChildIDDao;
    }

    Object readResolve() {
        return statUserLoginChildIDDao;
    }


    /**
     * 保存 child id 用户注册登录数
     *
     * @param date                        日期
     * @param appID                       应用id
     * @param childID                     马甲包id
     * @param childIDLoginCount           登录数
     * @param childIDGuestRegisterCount   游客注册数
     * @param childIDAccountRegisterCount 账号注册数
     * @param childIDPhoneRegisterCount   手机号注册数
     * @param childIDQQRegisterCount      QQ注册数
     * @param childIDWXRegisterCount      微信注册数
     * @param childIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveChildIDRegisterLoginCount(String date, int appID, long childID, long childIDLoginCount, long childIDGuestRegisterCount, long childIDAccountRegisterCount,
                                             long childIDPhoneRegisterCount, long childIDQQRegisterCount, long childIDWXRegisterCount, long childIDOtherRegisterCount, long childIDTotalRegisterCount) {

        String querySQL = "select id from stat_user_register_login_child_id where app_id = ? and child_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appID, childID, date});
        if (map == null || map.isEmpty()) {
            // 没有数据 插入一条
            String insertSQL = "insert into stat_user_register_login_child_id (app_id, child_id, login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, total_register_count, date)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?)";
            return executeSql(insertSQL, new Object[]{appID, childID, childIDLoginCount, childIDGuestRegisterCount, childIDAccountRegisterCount, childIDPhoneRegisterCount,
                    childIDQQRegisterCount, childIDWXRegisterCount, childIDOtherRegisterCount, childIDTotalRegisterCount, date});
        } else {
            // 数据存在 更新数据
            String sql = "update stat_user_register_login_child_id set login_count=?, guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , other_register_count = ?, total_register_count=?" +
                    " where app_id = ? and child_id = ? and date = ?";
            return executeSql(sql, new Object[]{childIDLoginCount, childIDGuestRegisterCount, childIDAccountRegisterCount, childIDPhoneRegisterCount, childIDQQRegisterCount,
                    childIDWXRegisterCount, childIDOtherRegisterCount, childIDTotalRegisterCount, appID, childID, date});
        }
    }


    /**
     * 保存 child id 用户分钟注册登录数
     *
     * @param time                        时间
     * @param appID                       应用id
     * @param childID                     马甲包id
     * @param childIDLoginCount           登录数
     * @param childIDGuestRegisterCount   游客注册数
     * @param childIDAccountRegisterCount 账号注册数
     * @param childIDPhoneRegisterCount   手机号注册数
     * @param childIDQQRegisterCount      QQ注册数
     * @param childIDWXRegisterCount      微信注册数
     * @param childIDTotalRegisterCount   注册总数
     * @return
     */
    public int saveChildIDMinuteRegisterLoginCount(String time, int appID, long childID, long childIDLoginCount, long childIDGuestRegisterCount, long childIDAccountRegisterCount,
                                                   long childIDPhoneRegisterCount, long childIDQQRegisterCount, long childIDWXRegisterCount, long childIDOtherRegisterCount, long childIDTotalRegisterCount) {

        String tableName = "stat_user_register_login_child_id_minute_" + time.substring(0, 7).replace("-", "");

        String querySql = "select id from " + tableName + " where app_id=? and child_id=? and time=?";
        Map<String, Object> map = queryForMap(querySql, new Object[]{appID, childID, time});
        if (map == null || map.isEmpty()) {
            // 没有数据 插入一条
            String insertSql = "insert into " + tableName + " (app_id, child_id,login_count, guest_register_count, account_register_count, phone_register_count, qq_register_count, wx_register_count, other_register_count, total_register_count, time)" +
                    " values (?,?,?,?,?,?,?,?,?,?,?)";
            return executeSql(insertSql, new Object[]{appID, childID, childIDLoginCount, childIDGuestRegisterCount, childIDAccountRegisterCount, childIDPhoneRegisterCount, childIDQQRegisterCount, childIDWXRegisterCount, childIDOtherRegisterCount, childIDTotalRegisterCount, time});
        } else {
            String updateSQL = "update " + tableName + " set login_count = ? , guest_register_count = ?, account_register_count = ?, phone_register_count = ?, qq_register_count = ?, wx_register_count = ? , other_register_count = ?, total_register_count=?" +
                    " where app_id = ? and child_id = ? and time = ?";
            return executeSql(updateSQL, new Object[]{childIDLoginCount, childIDGuestRegisterCount, childIDAccountRegisterCount, childIDPhoneRegisterCount, childIDQQRegisterCount,
                    childIDWXRegisterCount, childIDOtherRegisterCount, childIDTotalRegisterCount, appID, childID, time});
        }
    }
}
