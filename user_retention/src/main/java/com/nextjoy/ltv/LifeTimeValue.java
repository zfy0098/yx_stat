package com.nextjoy.ltv;

import com.nextjoy.ltv.dao.LifeTimeValueIntervalDao;
import com.nextjoy.ltv.dao.LifeTimeValuePayDao;
import com.nextjoy.ltv.dao.LifeTimeValueRegisterDao;
import com.nextjoy.retention.constant.Constant;
import com.nextjoy.retention.enums.LifeTimeValueDay;
import com.nextjoy.retention.utils.DateUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/1.
 *
 * @author Zhoufy
 */
public class LifeTimeValue implements Serializable {


    private LifeTimeValuePayDao lifeTimeValuePayDao = LifeTimeValuePayDao.getInstance();

    private LifeTimeValueRegisterDao lifeTimeValueRegisterDao = LifeTimeValueRegisterDao.getInstance();

    private LifeTimeValueIntervalDao lifeTimeValueIntervalDao = LifeTimeValueIntervalDao.getInstance();

    private void run(String nowDate) {

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Calendar calendar = Calendar.getInstance();
        List<String> list = new ArrayList<>(20);
        try {
            Date date;
            for (int day : Constant.LTV_DAYS) {
                date = sdf.parse(nowDate);
                calendar.setTime(date);
                calendar.add(Calendar.DAY_OF_MONTH, -day);
                date = calendar.getTime();
                list.add(sdf.format(date));
            }
        } catch (ParseException e) {
            System.exit(1);
        }

        String yesterday = list.get(0);

        /*
         *   保存注册数据
         */
        lifeTimeValueRegisterDao.saveLTVAppIDRegister(yesterday);
        lifeTimeValueRegisterDao.saveLTVChildIDRegister(yesterday);
        lifeTimeValueRegisterDao.saveLTVChannelIDRegister(yesterday);
        lifeTimeValueRegisterDao.saveLTVAppChannelIDRegister(yesterday);
        lifeTimeValueRegisterDao.saveLTVPackageIDRegister(yesterday);


        SparkConf sparkConf = new SparkConf().setAppName("user_ltv").setMaster("spark://172.26.110.193:7077");

        SparkSession sqlContext = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        sqlContext.sql("use yxstat");

        calculateLTV(sqlContext, list);

        calculateIntervalValue(sqlContext, nowDate);

        sqlContext.close();
    }

    private void calculateLTV(SparkSession sqlContext, List<String> list) {
        String appIDSQL = "select a.appid, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid from user_register where riqi = '%s' ) as  a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid";
        Map<String, Dataset<Row>> appMap = executeSQL(sqlContext, list, appIDSQL);
        for (String key : appMap.keySet()) {
            Dataset<Row> dataSet = appMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                long count = row.getLong(1);
                long money = row.getLong(2);
                String day = row.getString(3);
                if(count > 0 || money > 0){
                    lifeTimeValuePayDao.saveAppIDLTV(appid, count, money, day, key + "_pay_count", key + "_pay_amount");
                }
            }
        }

        String childIDSQL = "select a.appid, a.child_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid , appid, child_id from user_register where riqi = '%s' ) as  a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid , a.child_id";
        Map<String, Dataset<Row>> childMap = executeSQL(sqlContext, list, childIDSQL);
        for (String key : childMap.keySet()) {
            Dataset<Row> dataSet = childMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childId = row.getInt(1);
                long count = row.getLong(2);
                long money = row.getLong(3);
                String day = row.getString(4);
                if(count > 0 || money > 0) {
                    lifeTimeValuePayDao.saveChildIDLTV(appid, childId, count, money, day, key + "_pay_count", key + "_pay_amount");
                }
            }
        }

        String channelIDSQL = "select a.appid, a.child_id, a.channel_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id";
        Map<String, Dataset<Row>> channelIDMap = executeSQL(sqlContext, list, channelIDSQL);
        for (String key : channelIDMap.keySet()) {
            Dataset<Row> dataSet = channelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childId = row.getInt(1);
                int channelID = row.getInt(2);
                long count = row.getLong(3);
                long money = row.getLong(4);
                String day = row.getString(5);
                if(count > 0 || money > 0) {
                    lifeTimeValuePayDao.saveChannelIDLTV(appid, childId, channelID, count, money, day, key + "_pay_count", key + "_pay_amount");
                }
            }
        }

        String appChannelIDSQL = "select a.appid, a.child_id, a.channel_id, a.app_channel_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id, app_channel_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id";
        Map<String, Dataset<Row>> appChannelIDMap = executeSQL(sqlContext, list, appChannelIDSQL);
        for (String key : appChannelIDMap.keySet()) {
            Dataset<Row> dataSet = appChannelIDMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childId = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                long count = row.getLong(4);
                long money = row.getLong(5);
                String day = row.getString(6);
                if(count > 0 || money > 0) {
                    lifeTimeValuePayDao.saveAppChannelIDLTV(appid, childId, channelID, appChannelID, count, money, day, key + "_pay_count", key + "_pay_amount");
                }
            }
        }

        String packageIDSQL = "select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id, app_channel_id, package_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id";
        Map<String, Dataset<Row>> packageMap = executeSQL(sqlContext, list, packageIDSQL);
        for (String key : packageMap.keySet()) {
            Dataset<Row> dataSet = packageMap.get(key);
            for (Row row : dataSet.collectAsList()) {
                int appid = row.getInt(0);
                int childId = row.getInt(1);
                int channelID = row.getInt(2);
                int appChannelID = row.getInt(3);
                int packageID = row.getInt(4);
                long count = row.getLong(5);
                long money = row.getLong(6);
                String day = row.getString(7);
                if(count > 0 || money > 0) {
                    lifeTimeValuePayDao.savePackageIDLTV(appid, childId, channelID, appChannelID, packageID, count, money, day, key + "_pay_count", key + "_pay_amount");
                }
            }
        }
    }


    private void calculateIntervalValue(SparkSession sqlContext, String nowDate) {

        Map<Integer, List<Map<String, Object>>> appIDDay = lifeTimeValueIntervalDao.getAppIDDay();
        for (Map.Entry<Integer, List<Map<String, Object>>> entry : appIDDay.entrySet()) {
            String col = LifeTimeValueDay.getCol(entry.getKey());

            for (Map<String, Object> map : entry.getValue()) {
                String registerDay = map.getOrDefault("date", "").toString();

                String appIDSQL = "select a.appid, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid from user_register where riqi = '%s' ) as  a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid";
                Dataset<Row> dataSet = sqlContext.sql(String.format(appIDSQL, registerDay, registerDay, registerDay, nowDate));
                for (Row row : dataSet.collectAsList()) {
                    int appid = row.getInt(0);
                    long count = row.getLong(1);
                    long money = row.getLong(2);
                    String day = row.getString(3);
                    if(count > 0 || money > 0) {
                        lifeTimeValuePayDao.saveAppIDLTV(appid, count, money, day, col + "_pay_count", col + "_pay_amount");
                    }
                }
            }
        }

        Map<Integer, List<Map<String, Object>>> childIdDay = lifeTimeValueIntervalDao.getChildIDDay();
        for (Map.Entry<Integer, List<Map<String, Object>>> entry : childIdDay.entrySet()) {
            String col = LifeTimeValueDay.getCol(entry.getKey());

            for (Map<String, Object> map : entry.getValue()) {
                String registerDay = map.getOrDefault("date", "").toString();

                String childIDSQL = "select a.appid, a.child_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid , appid, child_id from user_register where riqi = '%s' ) as  a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid , a.child_id";
                Dataset<Row> dataSet = sqlContext.sql(String.format(childIDSQL, registerDay, registerDay, registerDay, nowDate));
                for (Row row : dataSet.collectAsList()) {
                    int appid = row.getInt(0);
                    int childId = row.getInt(1);
                    long count = row.getLong(2);
                    long money = row.getLong(3);
                    String day = row.getString(4);
                    if(count > 0 || money > 0) {
                        lifeTimeValuePayDao.saveChildIDLTV(appid, childId, count, money, day, col + "_pay_count", col + "_pay_amount");
                    }
                }
            }
        }

        Map<Integer, List<Map<String, Object>>> channelIDDay = lifeTimeValueIntervalDao.getChannelIDDay();
        for (Map.Entry<Integer, List<Map<String, Object>>> entry : channelIDDay.entrySet()) {
            String col = LifeTimeValueDay.getCol(entry.getKey());

            for (Map<String, Object> map : entry.getValue()) {
                String registerDay = map.getOrDefault("date", "").toString();

                String channelIDSQL = "select a.appid, a.child_id, a.channel_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id";
                Dataset<Row> dataSet = sqlContext.sql(String.format(channelIDSQL, registerDay, registerDay, registerDay, nowDate));
                for (Row row : dataSet.collectAsList()) {
                    int appid = row.getInt(0);
                    int childId = row.getInt(1);
                    int channelID = row.getInt(2);
                    long count = row.getLong(3);
                    long money = row.getLong(4);
                    String day = row.getString(5);
                    if(count > 0 || money > 0) {
                        lifeTimeValuePayDao.saveChannelIDLTV(appid, childId, channelID, count, money, day, col + "_pay_count", col + "_pay_amount");
                    }
                }
            }
        }

        Map<Integer, List<Map<String, Object>>> appChannelIDDay = lifeTimeValueIntervalDao.getAppChannelIDDay();
        for (Map.Entry<Integer, List<Map<String, Object>>> entry : appChannelIDDay.entrySet()) {
            String col = LifeTimeValueDay.getCol(entry.getKey());

            for (Map<String, Object> map : entry.getValue()) {
                String registerDay = map.getOrDefault("date", "").toString();

                String appChannelIDSQL = "select a.appid, a.child_id, a.channel_id, a.app_channel_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id, app_channel_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id";
                Dataset<Row> dataSet = sqlContext.sql(String.format(appChannelIDSQL, registerDay, registerDay, registerDay, nowDate));
                for (Row row : dataSet.collectAsList()) {
                    int appid = row.getInt(0);
                    int childId = row.getInt(1);
                    int channelID = row.getInt(2);
                    int appChannelID = row.getInt(3);
                    long count = row.getLong(4);
                    long money = row.getLong(5);
                    String day = row.getString(6);
                    if(count > 0 || money > 0) {
                        lifeTimeValuePayDao.saveAppChannelIDLTV(appid, childId, channelID, appChannelID, count, money, day, col + "_pay_count", col + "_pay_amount");
                    }
                }
            }
        }

        Map<Integer, List<Map<String, Object>>> packageIDDay = lifeTimeValueIntervalDao.getPackageIDDay();
        for (Map.Entry<Integer, List<Map<String, Object>>> entry : packageIDDay.entrySet()) {
            String col = LifeTimeValueDay.getCol(entry.getKey());

            for (Map<String, Object> map : entry.getValue()) {
                String registerDay = map.getOrDefault("date", "").toString();

                String packageIDSQL = "select a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id, count(distinct(a.uid)) as count , sum(money) as money ,'%s' from (select distinct(uid) as uid, appid, child_id, channel_id, app_channel_id, package_id from user_register where riqi = '%s' ) as a inner join (select * from pay_order where riqi between '%s' and '%s' and order_type = 1 ) as b on a.uid = b.uid group by a.appid, a.child_id, a.channel_id, a.app_channel_id, a.package_id";
                Dataset<Row> dataSet = sqlContext.sql(String.format(packageIDSQL, registerDay, registerDay, registerDay, nowDate));
                for (Row row : dataSet.collectAsList()) {
                    int appid = row.getInt(0);
                    int childId = row.getInt(1);
                    int channelID = row.getInt(2);
                    int appChannelID = row.getInt(3);
                    int packageID = row.getInt(4);
                    long count = row.getLong(5);
                    long money = row.getLong(6);
                    String day = row.getString(7);
                    if(count > 0 || money > 0) {
                        lifeTimeValuePayDao.savePackageIDLTV(appid, childId, channelID, appChannelID, packageID, count, money, day, col + "_pay_count", col + "_pay_amount");
                    }
                }
            }
        }
    }


    private Map<String, Dataset<Row>> executeSQL(SparkSession sqlContext, List<String> dateList, String sql) {
        Map<String, Dataset<Row>> map = new LinkedHashMap<>(Constant.days.length);
        if (dateList.size() > 0) {
            String yesterday = dateList.get(0);

            System.out.println(" yesterday :" + yesterday);

            for (int i = 0; i < dateList.size(); i++) {
                String day = dateList.get(i);
                String col = LifeTimeValueDay.getCol(Constant.LTV_DAYS[i]);
                System.out.println(day + " -- " + col + "------" + Constant.LTV_DAYS[i]);
                map.put(col, sqlContext.sql(String.format(sql, day, day, day, yesterday)));
            }
        }
        return map;
    }


    public static void main(String[] args) throws Exception {
        LifeTimeValue lifeTimeValue = new LifeTimeValue();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        String date = sdf.format(new Date());
        if (args.length > 0) {
            date = args[0];
        }


        List<String> list = DateUtils.getBetweenDates("2018-09-30" , "2018-10-31", DateUtils.YYYY_MM_DD);

        for (String day : list) {
            lifeTimeValue.run(day);
        }
//        lifeTimeValue.run(date);
    }
}
