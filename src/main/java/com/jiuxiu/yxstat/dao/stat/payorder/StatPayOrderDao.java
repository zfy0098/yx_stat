package com.jiuxiu.yxstat.dao.stat.payorder;

import com.jiuxiu.yxstat.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/7/9.
 *
 * @author ZhouFy
 */
public class StatPayOrderDao extends StatDataBase implements Serializable {

    private static StatPayOrderDao statPayOrderDao = new StatPayOrderDao();

    private StatPayOrderDao() {
    }

    public static StatPayOrderDao getInstance() {
        return statPayOrderDao;
    }

    Object readResolve() {
        return statPayOrderDao;
    }

    /**
     * 保存 appid 充值数据
     *
     * @param date           日期
     * @param appid          应用id
     * @param payUserCount   充值人数
     * @param payTotalAmount 充值总金额
     * @param payOrderCount  充值订单数
     * @return
     */
    public int saveAppIDPayOrder(String date, int appid, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_app_id where app_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_app_id (app_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_app_id set pay_user_count=?, pay_total_amount = ifnull(pay_total_amount, 0) + ?, pay_order_count = ifnull(pay_order_count , 0) + ? where app_id = ? and date = ?";
            return executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount, appid, date});
        }
    }

    /**
     * 保存 child id 充值数据
     *
     * @param date           日期
     * @param appid          应用id
     * @param childID        马甲包id
     * @param payUserCount   充值人数
     * @param payTotalAmount 充值总金额
     * @param payOrderCount  充值订单数
     * @return
     */
    public int saveChildIDPayOrder(String date, int appid, int childID, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_child_id where app_id = ? and child_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_child_id (app_id, child_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_child_id set pay_user_count = ?, pay_total_amount = ifnull(pay_total_amount, 0 ) + ?, pay_order_count = ifnull(pay_order_count, 0 ) + ?" +
                    " where  app_id = ? and child_id = ? and date = ?";
            return executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount,  appid, childID, date});
        }
    }


    /**
     * 保存通道 充值数据
     *
     * @param date           日期
     * @param appid          应用id
     * @param childID        马甲包id
     * @param channelID      渠道id
     * @param payUserCount   充值人数
     * @param payTotalAmount 充值总金额
     * @param payOrderCount  充值订单数
     * @return
     */
    public int saveChannelIDPayOrder(String date, int appid, int childID, int channelID, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_channel_id where app_id = ? and child_id = ? and channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_channel_id (app_id, child_id, channel_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_channel_id set pay_user_count = ?, pay_total_amount = ifnull(pay_total_amount, 0) + ?, pay_order_count = ifnull(pay_order_count,0) + ? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and date = ?";
            return executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount, appid, childID, channelID, date});
        }
    }


    /**
     * 保存子渠道充值数据
     *
     * @param date           日期
     * @param appid          应用id
     * @param childID        马甲包id
     * @param channelID      渠道id
     * @param appChannelID   子渠道id
     * @param payUserCount   充值人数
     * @param payTotalAmount 充值总金额
     * @param payOrderCount  充值订单数
     * @return
     */
    public int saveAppChannelIDPayOrder(String date, int appid, int childID, int channelID, int appChannelID, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_app_channel_id (app_id, child_id, channel_id, app_channel_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_app_channel_id set pay_user_count = ?, pay_total_amount = ifnull(pay_total_amount, 0) + ?, pay_order_count = ifnull(pay_order_count,0) + ? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
            return executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount, appid, childID, channelID, appChannelID, date});
        }
    }


    /**
     * 保存 包 充值数据
     *
     * @param date           日期
     * @param appid          应用id
     * @param childID        马甲包id
     * @param channelID      渠道id
     * @param appChannelID   子渠道id
     * @param packageID      包id
     * @param payUserCount   充值人数
     * @param payTotalAmount 充值总金额
     * @param payOrderCount  充值订单
     * @return
     */
    public int savePackageIDPayOrder(String date, int appid, int childID, int channelID, int appChannelID, int packageID, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_package_id (app_id, child_id, channel_id, app_channel_id, package_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?,?,?,?,?)";
            return executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_package_id set pay_user_count = ?, pay_total_amount = ifnull(pay_total_amount, 0) + ?, pay_order_count = ifnull(pay_order_count,0) + ? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            return executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount, appid, childID, channelID, appChannelID, packageID, date});
        }
    }


    /**
     *   保存 平台 充值信息
     * @param date
     * @param payUserCount
     * @param payTotalAmount
     * @param payOrderCount
     * @return
     */
    public int savePlatformPayOrder(String date, long payUserCount, long payTotalAmount, long payOrderCount){
        String querySQL = "select id from stat_pay_order where date = ?";
        Map<String,Object> map = queryForMap(querySQL , new Object[]{date});
        if(map == null || map.isEmpty()){
            String sql = "insert into stat_pay_order (pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?)";
            return executeSql(sql , new Object[]{payUserCount, payTotalAmount, payOrderCount, date});
        }else{
            String sql = "update stat_pay_order set pay_user_count = ?, pay_total_amount = ifnull(pay_total_amount, 0) + ?, pay_order_count = ifnull(pay_order_count,0) + ?  where date = ?";
            return executeSql(sql , new Object[]{payUserCount, payTotalAmount, payOrderCount, date});
        }
    }
}
