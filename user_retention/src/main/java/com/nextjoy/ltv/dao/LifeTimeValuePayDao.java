package com.nextjoy.ltv.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/2.
 *
 * @author Zhoufy
 */
public class LifeTimeValuePayDao extends StatDataBase implements Serializable {


    private static LifeTimeValuePayDao lifeTimeValuePayDao = null;


    private LifeTimeValuePayDao() {
    }


    public static LifeTimeValuePayDao getInstance() {
        if (lifeTimeValuePayDao == null) {
            lifeTimeValuePayDao = new LifeTimeValuePayDao();
        }
        return lifeTimeValuePayDao;
    }

    Object readResolve() {
        return lifeTimeValuePayDao;
    }


    public void saveAppIDLTV(int appid, long payCount, long payAmount, String day, String payCountCol, String payAmountCol) {
        String sql = "select id from stat_life_time_value_app_id where app_id = ? and date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, day});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_life_time_value_app_id (app_id, " + payCountCol + " , " + payAmountCol + " , date ) values (?,?,?,?)";
            executeSql(sql, new Object[]{appid, payCount, payAmount, day});
        } else {
            sql = "update stat_life_time_value_app_id set " + payCountCol + " = ? , " + payAmountCol + " = ? where app_id = ? and date = ?";
            executeSql(sql, new Object[]{payCount, payAmount, appid, day});
        }
    }


    public void saveChildIDLTV(int appid, int childID, long payCount, long payAmount, String day, String payCountCol, String payAmountCol) {
        String sql = "select id from stat_life_time_value_child_id where app_id = ? and child_id = ? and date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, day});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_life_time_value_child_id (app_id, child_id, " + payCountCol + " , " + payAmountCol + " , date ) values (?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, payCount, payAmount, day});
        } else {
            sql = "update stat_life_time_value_child_id set " + payCountCol + " = ? , " + payAmountCol + " = ? where app_id = ? and child_id = ? and date = ?";
            executeSql(sql, new Object[]{payCount, payAmount, appid, childID, day});
        }
    }

    public void saveChannelIDLTV(int appid, int childID, int channelID, long payCount, long payAmount, String day, String payCountCol, String payAmountCol) {
        String sql = "select id from stat_life_time_value_channel_id where app_id = ? and child_id = ? and channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, day});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_life_time_value_channel_id (app_id, child_id, channel_id, " + payCountCol + " , " + payAmountCol + " , date ) values (?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, payCount, payAmount, day});
        } else {
            sql = "update stat_life_time_value_channel_id set " + payCountCol + " = ? , " + payAmountCol + " = ? where app_id = ? and child_id = ? and channel_id = ? and date = ?";
            executeSql(sql, new Object[]{payCount, payAmount, appid, childID, channelID, day});
        }
    }


    public void saveAppChannelIDLTV(int appid, int childID, int channelID, int appChannelID, long payCount, long payAmount, String day, String payCountCol, String payAmountCol) {
        String sql = "select id from stat_life_time_value_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, day});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_life_time_value_app_channel_id (app_id, child_id, channel_id, app_channel_id , " + payCountCol + " , " + payAmountCol + " , date ) values (?,?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, payCount, payAmount, day});
        } else {
            sql = "update stat_life_time_value_app_channel_id set " + payCountCol + " = ? , " + payAmountCol + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
            executeSql(sql, new Object[]{payCount, payAmount, appid, childID, channelID, appChannelID, day});
        }
    }

    public void savePackageIDLTV(int appid, int childID, int channelID, int appChannelID, int packageID, long payCount, long payAmount, String day, String payCountCol, String payAmountCol) {
        String sql = "select id from stat_life_time_value_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, day});

        if (map == null || map.isEmpty()) {
            sql = "insert into stat_life_time_value_package_id (app_id, child_id, channel_id, app_channel_id , package_id, " + payCountCol + " , " + payAmountCol + " , date ) values (?,?,?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, payCount, payAmount, day});
        } else {
            sql = "update stat_life_time_value_package_id set " + payCountCol + " = ? , " + payAmountCol + " = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            executeSql(sql, new Object[]{payCount, payAmount, appid, childID, channelID, appChannelID, packageID, day});
        }
    }
}
