package com.jiuxiu.yxstat.dao.stat;

import com.jiuxiu.yxstat.db.StatDataBase;

/**
 * Created by ZhouFy on 2018/6/11.
 *
 * @author ZhouFy
 */
public class StatActivationStatisticsDao extends StatDataBase {

    private static StatActivationStatisticsDao statActivationStatisticsDao = null;

    private StatActivationStatisticsDao() {
    }

    public static StatActivationStatisticsDao getInstance() {
        if (statActivationStatisticsDao == null) {
            statActivationStatisticsDao = new StatActivationStatisticsDao();
        }
        return statActivationStatisticsDao;
    }

    /**
     *  保存 package id 启动数
     * @param obj
     * @return
     */
    public int savePackageIdStartUpCount(Object[] obj) {
        String sql = "insert into stat_package_id_device_active (package_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count = startup_count + ? ";
        return executeSql(sql, obj);
    }

    /**
     *   保存 child id 启动数
     * @param obj
     * @return
     */
    public int saveChildIDStartUpCount(Object[] obj) {
        String sql = "insert  into stat_child_id_device_active (child_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count = startup_count + ? ";
        return executeSql(sql, obj);
    }

    /**
     *   保存 app channel id启动数
     * @param obj
     * @return
     */
    public int saveAppChannelIdStartUpCount(Object[] obj) {
        String sql = "insert  into stat_app_channel_id_device_active (app_channel_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count = startup_count + ? ";
        return executeSql(sql, obj);
    }

    /**
     *  保存 channel id启动数
     * @param obj
     * @return
     */
    public int saveChannelIdStartUpCount(Object[] obj) {
        String sql = "insert  into stat_channel_id_device_active (channel_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count = startup_count + ? ";
        return executeSql(sql, obj);
    }

    /**
     *   保存app id 启动数
     * @param obj
     * @return
     */
    public int saveAppIdStartUpCount(Object[] obj) {
        String sql = "insert  into stat_app_id_device_active (app_id , startup_count, date) values  (?,?,date(now())) on  " +
                " duplicate key update startup_count = startup_count + ? ";
        return executeSql(sql, obj);
    }
}
