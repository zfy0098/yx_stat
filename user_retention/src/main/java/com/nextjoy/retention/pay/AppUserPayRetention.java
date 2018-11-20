package com.nextjoy.retention.pay;

import com.nextjoy.retention.constant.Constant;
import com.nextjoy.retention.enums.DayEnum;
import com.nextjoy.retention.pay.dao.UserPayRetention;
import com.nextjoy.retention.pay.dao.UserRegisterPay;
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
 * Created with IDEA by hadoop on 2018/8/17.
 *
 * @author Choufy
 */
public class AppUserPayRetention {


    private UserRegisterPay userRegisterPay = UserRegisterPay.getInstance();

    private UserPayRetention userPayRetention = UserPayRetention.getInstance();


    private void init(String nowDate) {

        List<String> dateList = DateUtils.dateAgo(nowDate);

        SparkConf sparkConf = new SparkConf().setAppName("pay_retention").setMaster("spark://172.26.110.193:7077");
        SparkSession sqlContext = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        sqlContext.sql("use yxstat");

        saveRegisterUserPayData(sqlContext, nowDate);

        saveAppIDUserPayRetentionData(sqlContext, dateList, nowDate);
        saveChildIDUserPayRetentionData(sqlContext, dateList, nowDate);
        saveChannelIDUserPayRetentionData(sqlContext, dateList, nowDate);
        saveAppChannelIDUserPayRetentionData(sqlContext, dateList, nowDate);
        savePackageIDUserPayRetentionData(sqlContext, dateList, nowDate);
    }

    private void saveAppIDUserPayRetentionData(SparkSession sqlContext, List<String> dateList, String nowDate) {
        String appIDSql = "select appid, count(1) as count , '%s' from (select a.appid, a.uid from " +
                " (select * from user_login where riqi='%s') as a inner join " +
                " (select a.uid from (select * from user_register where riqi = '%s') as a inner join (select * from pay_order where riqi='%s' and order_type=1) as b" +
                " on a.uid = b.uid group by a.uid) as b on a.uid = b.uid group by a.appid, a.uid) as c group by appid";

        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appIDSql, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                long count = row.getLong(1);
                String day = row.getString(2);
                userPayRetention.saveAppIDPayRetention(appid, count, day, key);
            }
        }
    }

    private void saveChildIDUserPayRetentionData(SparkSession sqlContext, List<String> dateList, String nowDate) {
        String appIDSql = "select appid, child_id, count(1) as count , '%s' from (select a.appid, a.child_id, a.uid from " +
                " (select * from user_login where riqi='%s') as a inner join " +
                " (select a.uid from (select * from user_register where riqi = '%s') as a inner join (select * from pay_order where riqi='%s' and order_type=1) as b" +
                " on a.uid = b.uid group by a.uid) as b on a.uid = b.uid group by a.appid, a.child_id, a.uid) as c group by appid, child_id";

        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appIDSql, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                long count = row.getLong(2);
                String day = row.getString(3);
                userPayRetention.saveChildIDPayRetention(appid, childID, count, day, key);
            }
        }
    }

    private void saveChannelIDUserPayRetentionData(SparkSession sqlContext, List<String> dateList, String nowDate) {
        String appIDSql = "select appid, child_id, channel_id, count(1) as count , '%s' from (select a.appid, a.child_id, a.channel_id, a.uid from " +
                " (select * from user_login where riqi='%s') as a inner join " +
                " (select a.uid from (select * from user_register where riqi = '%s') as a inner join (select * from pay_order where riqi='%s' and order_type=1) as b" +
                " on a.uid = b.uid group by a.uid) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.uid) as c group by appid, child_id, channel_id";

        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appIDSql, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                long count = row.getLong(3);
                String day = row.getString(4);
                userPayRetention.saveChannelIDPayRetention(appid, childID, channelID, count, day, key);
            }
        }
    }


    private void saveAppChannelIDUserPayRetentionData(SparkSession sqlContext, List<String> dateList, String nowDate) {
        String appIDSql = "select appid, child_id, channel_id, app_channel_id, count(1) as count , '%s' from (select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.uid from " +
                " (select * from user_login where riqi='%s') as a inner join " +
                " (select a.uid from (select * from user_register where riqi = '%s') as a inner join (select * from pay_order where riqi='%s' and order_type=1) as b" +
                " on a.uid = b.uid group by a.uid) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.uid) as c group by appid, child_id, channel_id, app_channel_id";

        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appIDSql, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                long count = row.getLong(4);
                String day = row.getString(5);
                userPayRetention.saveAppChannelIDPayRetention(appid, childID, channelID, appChannelID, count, day, key);
            }
        }
    }

    private void savePackageIDUserPayRetentionData(SparkSession sqlContext, List<String> dateList, String nowDate) {
        String appIDSql = "select appid, child_id, channel_id, app_channel_id, package_id, count(1) as count , '%s' from (select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, a.uid from " +
                " (select * from user_login where riqi='%s') as a inner join " +
                " (select a.uid from (select * from user_register where riqi = '%s') as a inner join (select * from pay_order where riqi='%s' and order_type=1) as b" +
                " on a.uid = b.uid group by a.uid) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, a.uid) as c group by appid, child_id, channel_id, app_channel_id, package_id";

        Map<String, Dataset<Row>> appidMap = executeSQL(sqlContext, dateList, appIDSql, nowDate);
        for (String key : appidMap.keySet()) {
            Dataset<Row> dataSet = appidMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childID = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                int packageID = row.getInt(4);
                long count = row.getLong(5);
                String day = row.getString(6);
                userPayRetention.savePackageIDPayRetention(appid, childID, channelID, appChannelID, packageID, count, day, key);
            }
        }
    }

    private Map<String, Dataset<Row>> executeSQL(SparkSession sqlContext, List<String> dateList, String sql, String startDay) {
        Map<String, Dataset<Row>> map = new LinkedHashMap<>(Constant.days.length);
        if (dateList.size() > 0) {
            for (int i = 0; i < dateList.size() - 1; i++) {
                String day = dateList.get(i);
                String col = DayEnum.getCol(Constant.days[i + 1]);
                map.put(col, sqlContext.sql(String.format(sql, day, startDay, day, day)));
            }
        }
        return map;
    }


    private void saveRegisterUserPayData(SparkSession sqlContext, String date) {

        String appidSQL = "select appid, count(1) as count, sum(money) as money from (select a.appid, a.uid ,sum(money) as money from (select  uid, appid , riqi  from user_register where riqi= '" + date + "' group by uid , appid , riqi) as a inner join pay_order as b on a.uid = b.uid where b.order_type=1 and a.riqi='" + date + "' and b.riqi='" + date + "' group by a.appid, a.uid) as c group by appid";
        Dataset<Row> dataset = sqlContext.sql(appidSQL);
        for (Row row : dataset.collectAsList()) {
            int appid = row.getInt(0);
            long count = row.getLong(1);
            long money = row.getLong(2);
            userRegisterPay.saveAppIDRegisterPay(appid, count, money, date);
        }

        String childIDSQL = "select appid, child_id, count(1) as count ,sum(money) as money from (select a.appid, a.child_id, a.uid ,sum(money) as money  from (select uid, appid, child_id, riqi from user_register where riqi= '" + date + "' group by uid, appid, child_id, riqi) as a inner join pay_order as b on a.uid = b.uid where b.order_type=1 and a.riqi='" + date + "' and b.riqi='" + date + "' group by a.appid, a.child_id, a.uid) as c group by appid, child_id";
        Dataset<Row> childIDdataSet = sqlContext.sql(childIDSQL);
        for (Row row : childIDdataSet.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            long count = row.getLong(2);
            long money = row.getLong(3);
            userRegisterPay.saveChildIDRegisterPay(appid, childID, count, money, date);
        }

        String channelIDSQL = "select appid, child_id, channel_id, count(1) as count, sum(money) as money from (select a.appid, a.child_id, a.channel_id, a.uid, sum(money) as money from (select uid, appid, child_id, channel_id, riqi from user_register where riqi= '" + date + "' group by uid , appid , child_id , channel_id, riqi) as a inner join pay_order as b on a.uid = b.uid where b.order_type=1 and a.riqi='" + date + "' and b.riqi='" + date + "' group by a.appid, a.child_id, a.channel_id, a.uid) as c group by appid, child_id, channel_id";
        Dataset<Row> channelDateSet = sqlContext.sql(channelIDSQL);
        for (Row row : channelDateSet.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            long count = row.getLong(3);
            long money = row.getLong(4);
            userRegisterPay.saveChannelIDRegisterPay(appid, childID, channelID, count, money, date);
        }

        String appChannelIDSQL = "select appid, child_id, channel_id, app_channel_id, count(1) as count, sum(money) as money from (select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.uid, sum(money) as money from (select uid, appid, child_id, channel_id, app_channel_id, riqi from user_register where riqi= '" + date + "' group by uid, appid, child_id, channel_id, app_channel_id, riqi) as a inner join pay_order as b  on a.uid = b.uid where b.order_type=1 and a.riqi='" + date + "' and b.riqi='" + date + "' group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.uid) as c group by appid, child_id, channel_id, app_channel_id";
        Dataset<Row> appChannelDataset = sqlContext.sql(appChannelIDSQL);
        for (Row row : appChannelDataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            long count = row.getLong(4);
            long money = row.getLong(5);
            userRegisterPay.saveAppChannelIDRegisterPay(appid, childID, channelID, appChannelID, count, money, date);
        }

        String packageIDSQL = "select appid, child_id, channel_id, app_channel_id, package_id, count(1) as count, sum(money) as money from (select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, a.uid, sum(money) as money from (select uid, appid, child_id, channel_id, app_channel_id, package_id, riqi from user_register where riqi= '" + date + "' group by uid, appid, child_id, channel_id, app_channel_id, package_id, riqi) as a inner join pay_order as b on a.uid = b.uid where b.order_type=1 and a.riqi='" + date + "' and b.riqi='" + date + "' group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, a.uid) as c group by appid, child_id, channel_id, app_channel_id, package_id";
        Dataset<Row> packageDataset = sqlContext.sql(packageIDSQL);
        for (Row row : packageDataset.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            int packageID = row.getInt(4);
            long count = row.getLong(5);
            long money = row.getLong(6);
            userRegisterPay.savePackageIDRegisterPay(appid, childID, channelID, appChannelID, packageID, count, money, date);
        }
    }

    public static void main(String[] args) throws Exception {
        AppUserPayRetention appUserPayRetention = new AppUserPayRetention();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String nowDate = sdf.format(new Date());
        if (args.length > 0) {
            nowDate = args[0];
        }
        appUserPayRetention.init(nowDate);
    }
}
