package com.nextjoy.ltv.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/1.
 *
 * @author Zhoufy
 * <p>
 * 保存 ltv 表中的注册数据
 */
public class LifeTimeValueRegisterDao extends StatDataBase implements Serializable {


    private static LifeTimeValueRegisterDao lifeTimeValueRegisterDao = null;

    private LifeTimeValueRegisterDao() {
    }

    public static LifeTimeValueRegisterDao getInstance() {
        if (lifeTimeValueRegisterDao == null) {
            lifeTimeValueRegisterDao = new LifeTimeValueRegisterDao();
        }
        return lifeTimeValueRegisterDao;
    }

    Object readResolve() {
        return lifeTimeValueRegisterDao;
    }


    public void saveLTVAppIDRegister(String date) {

        String sql = "select * from stat_user_retention_rate_app_id where register_date = ?";
        List<Map<String, Object>> list = queryForList(sql, new Object[]{date});

        List<Object[]> paramsList = new ArrayList<>();

        for (Map<String, Object> map : list) {
            String appid = map.getOrDefault("app_id", "").toString();
            String registerCount = map.getOrDefault("register_count", "").toString();
            sql = "select id from stat_life_time_value_app_id where app_id = ? and date = ?";
            Map<String, Object> ltvmap = queryForMap(sql, new Object[]{appid, date});

            if (ltvmap == null || ltvmap.isEmpty()) {
                paramsList.add(new Object[]{appid, registerCount, date});
            } else {
                String update = "update stat_life_time_value_app_id set register_count = ? where app_id = ? and date = ?";
                executeSql(update, new Object[]{registerCount, appid, date});
            }
        }
        if (list.size() > 0) {
            String save = "insert into stat_life_time_value_app_id (app_id, register_count, date) values (?,?,?)";
            executeBatchSql(save, paramsList);
        }
    }


    public void saveLTVChildIDRegister(String date) {
        String sql = "select * from stat_user_retention_rate_child_id where register_date = ? ";
        List<Map<String, Object>> list = queryForList(sql, new Object[]{date});
        List<Object[]> paramsList = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String appid = map.getOrDefault("app_id", "").toString();
            String childID = map.getOrDefault("child_id", "").toString();
            String registerCount = map.getOrDefault("register_count", "").toString();
            sql = "select id from stat_life_time_value_child_id where app_id = ? and child_id = ? and date = ?";
            Map<String, Object> ltvmap = queryForMap(sql, new Object[]{appid, childID, date});

            if (ltvmap == null || ltvmap.isEmpty()) {
                paramsList.add(new Object[]{appid, childID, registerCount, date});
            } else {
                String update = "update stat_life_time_value_child_id set register_count = ? where app_id = ? and child_id = ? and date = ?";
                executeSql(update, new Object[]{registerCount, appid, childID, date});
            }
        }

        if (list.size() > 0) {
            String save = "insert into stat_life_time_value_child_id (app_id, child_id, register_count, date) values (?,?,?,?)";
            executeBatchSql(save, paramsList);
        }
    }


    public void saveLTVChannelIDRegister(String date) {
        String sql = "select * from stat_user_retention_rate_channel_id where register_date = ? ";
        List<Map<String, Object>> list = queryForList(sql, new Object[]{date});
        List<Object[]> paramsList = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String appid = map.getOrDefault("app_id", "").toString();
            String childID = map.getOrDefault("child_id", "").toString();
            String channelID = map.getOrDefault("channel_id", "").toString();
            String registerCount = map.getOrDefault("register_count", "").toString();
            sql = "select id from stat_life_time_value_channel_id where app_id = ? and child_id = ? and channel_id = ? and date = ?";
            Map<String, Object> ltvmap = queryForMap(sql, new Object[]{appid, childID, channelID, date});

            if (ltvmap == null || ltvmap.isEmpty()) {
                paramsList.add(new Object[]{appid, childID, channelID, registerCount, date});
            } else {
                String update = "update stat_life_time_value_channel_id set register_count = ? where app_id = ? and child_id =? and channel_id = ? and date = ?";
                executeSql(update, new Object[]{registerCount, appid, childID, channelID, date});
            }
        }

        if (list.size() > 0) {
            String save = "insert into stat_life_time_value_channel_id (app_id, child_id, channel_id, register_count, date) values (?,?,?,?,?)";
            executeBatchSql(save, paramsList);
        }
    }


    public void saveLTVAppChannelIDRegister(String date) {
        String sql = "select * from stat_user_retention_rate_app_channel_id where register_date = ? ";
        List<Map<String, Object>> list = queryForList(sql, new Object[]{date});
        List<Object[]> paramsList = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String appid = map.getOrDefault("app_id", "").toString();
            String childID = map.getOrDefault("child_id", "").toString();
            String channelID = map.getOrDefault("channel_id", "").toString();
            String appChannelID = map.getOrDefault("app_channel_id", "").toString();
            String registerCount = map.getOrDefault("register_count", "").toString();
            sql = "select id from stat_life_time_value_app_channel_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and date = ?";
            Map<String, Object> ltvmap = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, date});

            if (ltvmap == null || ltvmap.isEmpty()) {
                paramsList.add(new Object[]{appid, childID, channelID, appChannelID, registerCount, date});
            } else {
                String update = "update stat_life_time_value_app_channel_id set register_count = ? where app_id = ? and child_id =? and channel_id = ? and app_channel_id = ? and date = ?";
                executeSql(update, new Object[]{registerCount, appid, childID, channelID, appChannelID, date});
            }
        }

        if (list.size() > 0) {
            String save = "insert into stat_life_time_value_app_channel_id (app_id, child_id, channel_id, app_channel_id, register_count, date) values (?,?,?,?,?,?)";
            executeBatchSql(save, paramsList);
        }
    }


    public void saveLTVPackageIDRegister(String date) {
        String sql = "select * from stat_user_retention_rate_package_id where register_date = ? ";
        List<Map<String, Object>> list = queryForList(sql, new Object[]{date});
        List<Object[]> paramsList = new ArrayList<>();
        for (Map<String, Object> map : list) {
            String appid = map.getOrDefault("app_id", "").toString();
            String childID = map.getOrDefault("child_id", "").toString();
            String channelID = map.getOrDefault("channel_id", "").toString();
            String appChannelID = map.getOrDefault("app_channel_id", "").toString();
            String packageID = map.getOrDefault("package_id", "").toString();
            String registerCount = map.getOrDefault("register_count", "").toString();
            sql = "select id from stat_life_time_value_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            Map<String, Object> ltvmap = queryForMap(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, date});

            if (ltvmap == null || ltvmap.isEmpty()) {
                paramsList.add(new Object[]{appid, childID, channelID, appChannelID, packageID, registerCount, date});
            } else {
                String update = "update stat_life_time_value_package_id set register_count = ? where app_id = ? and child_id =? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
                executeSql(update, new Object[]{registerCount, appid, childID, channelID, appChannelID, packageID, date});
            }
        }

        if (list.size() > 0) {
            String save = "insert into stat_life_time_value_package_id (app_id, child_id, channel_id, app_channel_id, package_id, register_count, date) values (?,?,?,?,?,?,?)";
            executeBatchSql(save, paramsList);
        }
    }
}
