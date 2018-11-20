package com.nextjoy.ltv.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/13.
 *
 * @author Zhoufy
 */
public class LifeTimeValueIntervalDao extends StatDataBase implements Serializable {

    private static LifeTimeValueIntervalDao lifeTimeValueIntervalDao = null;

    private LifeTimeValueIntervalDao() {
    }

    public static LifeTimeValueIntervalDao getInstance() {
        if (lifeTimeValueIntervalDao == null) {
            lifeTimeValueIntervalDao = new LifeTimeValueIntervalDao();
        }
        return lifeTimeValueIntervalDao;
    }


    Object readResolve() {
        return lifeTimeValueIntervalDao;
    }


    public Map<Integer, List<Map<String, Object>>> getAppIDDay() {

        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
        String sql = "select date from stat_life_time_value_app_id where datediff(date(now()) , date) < 15 and datediff(date(now()) , date) > 7 group by date";
        map.put(15, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_id where datediff(date(now()) , date) < 30 and datediff(date(now()) , date) > 15 group by date";
        map.put(30, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_id where datediff(date(now()) , date) < 60 and datediff(date(now()) , date) > 30 group by date";
        map.put(60, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_id where datediff(date(now()) , date) < 90 and datediff(date(now()) , date) > 60 group by date";
        map.put(90, queryForList(sql, null));

        return map;
    }


    public Map<Integer, List<Map<String, Object>>> getChildIDDay() {
        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
        String sql = "select date from stat_life_time_value_child_id where datediff(date(now()) , date) < 15 and datediff(date(now()) , date) > 7 group by date";
        map.put(15, queryForList(sql, null));

        sql = "select date from stat_life_time_value_child_id where datediff(date(now()) , date) < 30 and datediff(date(now()) , date) > 15 group by date";
        map.put(30, queryForList(sql, null));

        sql = "select date from stat_life_time_value_child_id where datediff(date(now()) , date) < 60 and datediff(date(now()) , date) > 30 group by date";
        map.put(60, queryForList(sql, null));

        sql = "select date from stat_life_time_value_child_id where datediff(date(now()) , date) < 90 and datediff(date(now()) , date) > 60 group by date";
        map.put(90, queryForList(sql, null));

        return map;
    }


    public Map<Integer, List<Map<String, Object>>> getChannelIDDay() {
        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
        String sql = "select date from stat_life_time_value_channel_id where datediff(date(now()) , date) < 15 and datediff(date(now()) , date) > 7 group by date";
        map.put(15, queryForList(sql, null));

        sql = "select date from stat_life_time_value_channel_id where datediff(date(now()) , date) < 30 and datediff(date(now()) , date) > 15 group by date";
        map.put(30, queryForList(sql, null));

        sql = "select date from stat_life_time_value_channel_id where datediff(date(now()) , date) < 60 and datediff(date(now()) , date) > 30 group by date";
        map.put(60, queryForList(sql, null));

        sql = "select date from stat_life_time_value_channel_id where datediff(date(now()) , date) < 90 and datediff(date(now()) , date) > 60 group by date";
        map.put(90, queryForList(sql, null));

        return map;
    }


    public Map<Integer, List<Map<String, Object>>> getAppChannelIDDay() {
        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
        String sql = "select date from stat_life_time_value_app_channel_id where datediff(date(now()) , date) < 15 and datediff(date(now()) , date) > 7 group by date";
        map.put(15, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_channel_id where datediff(date(now()) , date) < 30 and datediff(date(now()) , date) > 15 group by date";
        map.put(30, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_channel_id where datediff(date(now()) , date) < 60 and datediff(date(now()) , date) > 30 group by date";
        map.put(60, queryForList(sql, null));

        sql = "select date from stat_life_time_value_app_channel_id where datediff(date(now()) , date) < 90 and datediff(date(now()) , date) > 60 group by date";
        map.put(90, queryForList(sql, null));

        return map;
    }


    public Map<Integer, List<Map<String, Object>>> getPackageIDDay() {
        Map<Integer, List<Map<String, Object>>> map = new HashMap<>();
        String sql = "select date from stat_life_time_value_package_id where datediff(date(now()) , date) < 15 and datediff(date(now()) , date) > 7 group by date";
        map.put(15, queryForList(sql, null));

        sql = "select date from stat_life_time_value_package_id where datediff(date(now()) , date) < 30 and datediff(date(now()) , date) > 15 group by date";
        map.put(30, queryForList(sql, null));

        sql = "select date from stat_life_time_value_package_id where datediff(date(now()) , date) < 60 and datediff(date(now()) , date) > 30 group by date";
        map.put(60, queryForList(sql, null));

        sql = "select date from stat_life_time_value_package_id where datediff(date(now()) , date) < 90 and datediff(date(now()) , date) > 60 group by date";
        map.put(90, queryForList(sql, null));

        return map;
    }
}
