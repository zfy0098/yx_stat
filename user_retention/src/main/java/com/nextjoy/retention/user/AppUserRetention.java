package com.nextjoy.retention.user;

import com.nextjoy.retention.constant.Constant;
import com.nextjoy.retention.enums.DayEnum;
import com.nextjoy.retention.user.dao.UserRegister;
import com.nextjoy.retention.user.dao.UserRetention;
import com.nextjoy.retention.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Hello world!
 *
 * @author hadoop
 */
public class AppUserRetention {

    private UserRetention userRetention = UserRetention.getInstance();

    private UserRegister userRegister = UserRegister.getInstance();

    private void init(String nowDate) {

        SparkConf sparkConf = new SparkConf().setAppName("user_retention").setMaster("spark://172.26.110.193:7077");

        SparkSession sqlContext = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        sqlContext.sql("use yxstat");

        List<String> dateList = DateUtils.dateAgo(nowDate);

        String date = dateList.get(1);
        saveRegisterData(sqlContext, date);

        String appidSQL = "select appid, count(1) as count, '%s' from (select appid, a.uid from user_login as a inner join (select uid from user_register where riqi='%s') as b" +
                " on a.uid=b.uid where riqi='%s' group by appid, a.uid)" +
                " as c group by appid";
        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appidSQL);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                long count = row.getLong(1);
                String day = row.getString(2);
                userRetention.saveAppIDRetention(appid, count, day, key);
            }
        }

        String childIDSQL = "select appid, child_id, count(1) as count, '%s' from " +
                " (select appid, child_id, a.uid from user_login as a inner join (select uid from user_register where riqi='%s') as b" +
                "  on a.uid=b.uid where riqi='%s'  group by appid, child_id, a.uid ) as c group by appid, child_id ";
        Map<String, Dataset<Row>> childIDMap = executeSQL(sqlContext, dateList, childIDSQL);
        for (String key : childIDMap.keySet()) {
            Dataset<Row> dataSet = childIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                long count = row.getLong(2);
                String day = row.getString(3);
                userRetention.saveChildIDRetention(appid, childID, count, day, key);
            }
        }

        String channelIDSQL = "select appid, child_id, channel_id, count(1) as count, '%s' from " +
                " (select appid ,child_id, channel_id, a.uid from user_login as a inner join (select uid from user_register where riqi='%s' ) as b " +
                " on a.uid=b.uid where riqi='%s' group by appid, child_id, channel_id, a.uid) as c group by appid, child_id, channel_id ";
        Map<String, Dataset<Row>> channelIDMap = executeSQL(sqlContext, dateList, channelIDSQL);
        for (String key : channelIDMap.keySet()) {
            Dataset<Row> dataSet = channelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                long count = row.getLong(3);
                String day = row.getString(4);
                userRetention.saveChannelIDRetention(appid, childID, channelID, count, day, key);
            }
        }

        String appChannelIDSQL = "select appid, child_id, channel_id, app_channel_id, count(1) as count, '%s' from " +
                " (select appid ,child_id, channel_id, app_channel_id, a.uid from user_login as a inner join (select uid from user_register where riqi='%s') as b" +
                "  on a.uid=b.uid where riqi='%s' group by appid, child_id, channel_id, app_channel_id, a.uid )" +
                " as c group by appid, child_id, channel_id, app_channel_id";
        Map<String, Dataset<Row>> appChannelIDMap = executeSQL(sqlContext, dateList, appChannelIDSQL);
        for (String key : appChannelIDMap.keySet()) {
            Dataset<Row> dataSet = appChannelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                long count = row.getLong(4);
                String day = row.getString(5);
                userRetention.saveAppChannelIDRetention(appid, childID, channelID, appChannelID, count, day, key);
            }
        }

        String packageIDSQL = "select appid, child_id, channel_id, app_channel_id, package_id, count(1) as count, '%s' from" +
                " (select appid ,child_id, channel_id, app_channel_id, package_id, a.uid from user_login as a inner join " +
                " (select uid from user_register where riqi='%s' ) as b" +
                " on a.uid=b.uid where riqi='%s' group by appid, child_id, channel_id, app_channel_id, package_id, a.uid )" +
                " as c group by appid, child_id, channel_id, app_channel_id, package_id";
        Map<String, Dataset<Row>> packageIDMap = executeSQL(sqlContext, dateList, packageIDSQL);
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
                userRetention.savePackageIDRetention(appid, childID, channelID, appChannelID, packageID, count, day, key);
            }
        }
        sqlContext.close();
    }

    private void saveRegisterData(SparkSession sqlContext, String date) {

        String appidSQL = "select appid, count(1) as count , '" + date + "' from user_register where riqi='" + date + "' group by appid";
        Dataset<Row> dataset = sqlContext.sql(appidSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            long count = row.getLong(1);
            userRegister.saveAppIDRegister(appid, count, date);
        }

        String childIDSQL = "select appid, child_id, count(1) as count, '" + date + "' from user_register where riqi='" + date + "' group by appid , child_id";
        dataset = sqlContext.sql(childIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            long count = row.getLong(2);
            userRegister.saveChildIDRegister(appid, childID, count, date);
        }

        String channelIDSQL = "select appid, child_id, channel_id, count(1) as count, '" + date + "' from user_register where riqi='" + date + "' group by appid , child_id, channel_id";
        dataset = sqlContext.sql(channelIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            long count = row.getLong(3);
            userRegister.saveChannelIDRegister(appid, childID, channelID, count, date);
        }

        String appChannelIDSQL = "select appid, child_id, channel_id, app_channel_id, count(1) as count, '" + date + "' from user_register where riqi='" + date + "' group by appid, child_id, channel_id, app_channel_id";
        dataset = sqlContext.sql(appChannelIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            long count = row.getLong(4);
            userRegister.saveAppChannelIDRegister(appid, childID, channelID, appChannelID, count, date);
        }

        String packageIDSQL = "select appid, child_id, channel_id, app_channel_id, package_id, count(1) as count, '" + date + "' from user_register where riqi='" + date + "' group by appid , child_id, channel_id , app_channel_id , package_id ";
        dataset = sqlContext.sql(packageIDSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            int packageID = row.getInt(4);
            long count = row.getLong(5);
            userRegister.savePackageIDRegister(appid, childID, channelID, appChannelID, packageID, count, date);
        }
    }


    /**
     * 执行 spark-sql  返回查询结果
     *
     * @param sqlContext
     * @param dateList
     * @param sql
     * @return
     */
    private Map<String, Dataset<Row>> executeSQL(SparkSession sqlContext, List<String> dateList, String sql) {
        Map<String, Dataset<Row>> map = new LinkedHashMap<>(Constant.days.length);
        if (dateList.size() > 0) {
            String yesterday = dateList.get(0);
            for (int i = 1; i < dateList.size(); i++) {
                String day = dateList.get(i);
                String col = DayEnum.getCol(Constant.days[i]);

                map.put(col, sqlContext.sql(String.format(sql, day, day, yesterday)));
            }
        }
        return map;
    }



    public static void main(String[] args) {
        AppUserRetention appUserRetention = new AppUserRetention();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String nowDate = sdf.format(new Date());
        if (args.length > 0) {
            nowDate = args[0];
        }
        appUserRetention.init(nowDate);
    }
}
