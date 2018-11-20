package com.nextjoy.retention.device;

import com.nextjoy.retention.constant.Constant;
import com.nextjoy.retention.device.dao.DeviceActivateDao;
import com.nextjoy.retention.device.dao.DeviceRetentionDao;
import com.nextjoy.retention.enums.DayEnum;
import com.nextjoy.retention.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by hadoop on 2018/10/27.
 *
 * @author Zhoufy
 */
public class DeviceRetention implements Serializable {

    private DeviceActivateDao deviceActivateDao = DeviceActivateDao.getInstance();

    private DeviceRetentionDao deviceRetentionDao = DeviceRetentionDao.getInstance();

    private void run(String nowDate){

        List<String> dateList = DateUtils.dateAgo(nowDate);
        String date = dateList.get(0);

        //spark://172.26.110.193:7077

        SparkConf sparkConf = new SparkConf().setAppName("device_retention").setMaster("spark://172.26.110.193:7077");

        SparkSession sqlContext = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        sqlContext.sql("use yxstat");

        saveDeviceActivateData(sqlContext, date);

        String appidSQL = "select appid, count(distinct(imei)) as count, '%s' from" +
                " (select b.appid, a.imei " +
                "  from device_startup as a inner join (select appid, imei from device_active where riqi= '%s' " +
                " group by imei, appid ) as b " +
                " on a.imei=b.imei and a.appid=b.appid " +
                " where a.riqi='%s' group by b.appid, a.imei ) " +
                " as c group by appid";
        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appidSQL, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                long count = row.getLong(1);
                String day = row.getString(2);
                deviceRetentionDao.saveAppIDDeviceRetention(appid, count, day, key);
            }
        }

        String childIDSQL = "select appid, child_id, count(distinct(imei)) as count, '%s' from" +
                " (select b.appid, b.child_id, a.imei " +
                "  from device_startup as a inner join (select appid ,child_id, imei from device_active where riqi= '%s' " +
                " group by imei, appid, child_id ) as b " +
                " on a.imei=b.imei and a.appid=b.appid " +
                " where a.riqi='%s' group by b.appid, b.child_id, a.imei ) " +
                " as c group by appid, child_id";
        Map<String, Dataset<Row>> childIDMap = executeSQL(sqlContext, dateList, childIDSQL, nowDate);
        for (String key : childIDMap.keySet()) {
            Dataset<Row> dataSet = childIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                long count = row.getLong(2);
                String day = row.getString(3);
                deviceRetentionDao.saveChildIDDeviceRetention(appid, childID, count, day, key);
            }
        }

        String channelIDSQL = "select appid, child_id, channel_id, count(distinct(imei)) as count, '%s' from" +
                " (select b.appid, b.child_id, b.channel_id, a.imei " +
                "  from device_startup as a inner join (select appid ,child_id, channel_id, imei from device_active where riqi= '%s' " +
                " group by imei, appid, child_id, channel_id ) as b " +
                " on a.imei=b.imei and a.appid=b.appid " +
                " where a.riqi='%s' group by b.appid, b.child_id, b.channel_id, a.imei ) " +
                " as c group by appid, child_id, channel_id";
        Map<String, Dataset<Row>> channelIDMap = executeSQL(sqlContext, dateList, channelIDSQL, nowDate);
        for (String key : channelIDMap.keySet()) {
            Dataset<Row> dataSet = channelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                long count = row.getLong(3);
                String day = row.getString(4);
                deviceRetentionDao.saveChannelIDDeviceRetention(appid, childID, channelID, count, day, key);
            }
        }

        String appChannelIDSQL = "select appid, child_id, channel_id, app_channel_id, count(distinct(imei)) as count, '%s' from" +
                " (select b.appid, b.child_id, b.channel_id, b.app_channel_id, a.imei " +
                "  from device_startup as a inner join (select appid ,child_id, channel_id, app_channel_id, imei from device_active where riqi= '%s' " +
                " group by imei, appid, child_id, channel_id, app_channel_id ) as b " +
                " on a.imei=b.imei and a.appid=b.appid " +
                " where a.riqi='%s' group by b.appid, b.child_id, b.channel_id, b.app_channel_id, a.imei ) " +
                " as c group by appid, child_id, channel_id, app_channel_id";
        Map<String, Dataset<Row>> appChannelIDMap = executeSQL(sqlContext, dateList, appChannelIDSQL, nowDate);
        for (String key : appChannelIDMap.keySet()) {
            Dataset<Row> dataSet = appChannelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                long count = row.getLong(4);
                String day = row.getString(5);
                deviceRetentionDao.saveAppChannelIDDeviceRetention(appid, childID, channelID, appChannelID, count, day, key);
            }
        }

        String packageIDSQL = "select appid, child_id, channel_id, app_channel_id, package_id, count(distinct(imei)) as count, '%s' from" +
                " (select b.appid, b.child_id, b.channel_id, b.app_channel_id, b.package_id, a.imei " +
                "  from device_startup as a inner join (select appid ,child_id, channel_id, app_channel_id, package_id, imei from device_active where riqi= '%s' " +
                " group by imei, appid, child_id, channel_id, app_channel_id, package_id ) as b " +
                " on a.imei=b.imei and a.appid=b.appid " +
                " where a.riqi='%s' group by b.appid, b.child_id, b.channel_id, b.app_channel_id, b.package_id, a.imei ) " +
                " as c group by appid, child_id, channel_id, app_channel_id, package_id";
        Map<String, Dataset<Row>> packageIDMap = executeSQL(sqlContext, dateList, packageIDSQL, nowDate);
        for (String key : packageIDMap.keySet()) {
            Dataset<Row> dataSet = packageIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                int packageID = row.getInt(4);
                long count = row.getLong(5);
                String day = row.getString(6);
                deviceRetentionDao.savePackageIDDeviceRetention(appid, childID, channelID, appChannelID, packageID, count, day, key);
            }
        }
        sqlContext.close();


    }


    /**
     * 执行 spark-sql  返回查询结果
     *
     * @param sqlContext
     * @param dateList
     * @param sql
     * @return
     */
    private Map<String, Dataset<Row>> executeSQL(SparkSession sqlContext, List<String> dateList, String sql ,  String startDate) {
        Map<String, Dataset<Row>> map = new LinkedHashMap<>(Constant.days.length);
        if (dateList.size() > 0) {
            for (int i = 0; i < dateList.size() - 1; i++) {
                String day = dateList.get(i);
                String col = DayEnum.getCol(Constant.days[i + 1]);
                map.put(col, sqlContext.sql(String.format(sql, day, day, startDate)));
            }
        }
        return map;
    }


    private void saveDeviceActivateData(SparkSession sqlContext, String date) {

        String appidSQL = "select appid, count(distinct(imei)) as count , '" + date + "' from device_active where riqi='" + date + "' group by appid";
        Dataset<Row> dataset = sqlContext.sql(appidSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            long count = row.getLong(1);
            deviceActivateDao.saveAppidDeviceActivate(appid, count, date);
        }

        String childIDSQL = "select appid, child_id, count(distinct(imei)) as count, '" + date + "' from device_active where riqi='" + date + "' group by appid , child_id";
        dataset = sqlContext.sql(childIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            long count = row.getLong(2);
            deviceActivateDao.saveChildIDDeviceActivate(appid, childID, count, date);
        }

        String channelIDSQL = "select appid, child_id, channel_id, count(distinct(imei)) as count, '" + date + "' from device_active where riqi='" + date + "' group by appid , child_id, channel_id";
        dataset = sqlContext.sql(channelIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            long count = row.getLong(3);
            deviceActivateDao.saveChannelIDDeviceActivate(appid, childID, channelID, count, date);
        }

        String appChannelIDSQL = "select appid, child_id, channel_id, app_channel_id, count(distinct(imei)) as count, '" + date + "' from device_active where riqi='" + date + "' group by appid, child_id, channel_id, app_channel_id";
        dataset = sqlContext.sql(appChannelIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            long count = row.getLong(4);
            deviceActivateDao.saveAppChannelIDDeviceActivate(appid, childID, channelID, appChannelID, count, date);
        }

        String packageIDSQL = "select appid, child_id, channel_id, app_channel_id, package_id, count(distinct(imei)) as count, '" + date + "' from device_active where riqi='" + date + "' group by appid , child_id, channel_id , app_channel_id , package_id ";
        dataset = sqlContext.sql(packageIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            int packageID = row.getInt(4);
            long count = row.getLong(5);
            deviceActivateDao.savePackageIDDeviceActivate(appid, childID, channelID, appChannelID, packageID, count, date);
        }
    }


    public static void main(String[] args) throws Exception {
        DeviceRetention deviceRetention = new DeviceRetention();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String nowDate = sdf.format(new Date());
        if (args.length > 0) {
            nowDate = args[0];
        }

        deviceRetention.run(nowDate);

//        List<String> list = DateUtils.getBetweenDates("2018-10-25" , nowDate , "yyyy-MM-dd");
//
//        for (String day :  list) {
//            deviceRetention.run(day);
//
//        }

    }
}
