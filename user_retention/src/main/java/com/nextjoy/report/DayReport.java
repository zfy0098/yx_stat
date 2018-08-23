package com.nextjoy.report;

import com.nextjoy.retention.db.StatDataBase;
import com.nextjoy.retention.utils.DateUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by hadoop on 2018/8/21.
 *
 * @author Choufy
 */
public class DayReport extends StatDataBase {


    private static Map<Integer, String> map = new HashMap<>();

    static{
        map.put(1 , "day2");
        map.put(2 , "day3");
        map.put(6 , "day7");
        map.put(14, "day15");
    }

    private void init(String date){


        String year = date.split("-")[0];

        String appIdSQL = "insert into stat_report_forms_day_app_id (app_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) select a.app_id, ifnull(b.new_device_count,0), ifnull(a.total_register_count,0), ifnull(a.login_count,0), ifnull(c.register_count,0), ifnull(c.money,0), ifnull(d.pay_user_count,0)-ifnull(c.register_count,0), ifnull(d.pay_total_amount,0)-ifnull(c.money,0), ifnull(d.pay_user_count,0), ifnull(d.pay_total_amount,0), a.date from (select date, app_id, login_count, total_register_count from stat_user_register_login_app_id where date = '" + date + "') as a inner join (select * from stat_app_id_device_active where date = '" + date + "') as b on a.app_id = b.app_id  and a.date = b.date left join (select * from stat_user_pay_retention_rate_app_id where register_date = '" + date + "') as c on a.app_id=c.app_id and a.date=c.register_date left JOIN (select * from stat_pay_order_app_id where date='" + date + "') as d on a.app_id=d.app_id and a.date = d.date ";
        executeSql(appIdSQL , null);

        String childSQL = "insert into stat_report_forms_day_child_id (app_id, child_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) select a.app_id, a.child_id, ifnull(b.new_device_count,0), ifnull(a.total_register_count,0), ifnull(a.login_count,0), ifnull(c.register_count,0), ifnull(c.money,0), ifnull(d.pay_user_count,0)-ifnull(c.register_count,0), ifnull(d.pay_total_amount,0)-ifnull(c.money,0), ifnull(d.pay_user_count,0), ifnull(d.pay_total_amount,0), a.date from (select date, app_id, child_id, login_count, total_register_count from stat_user_register_login_child_id where date = '" + date + "') as a inner join (select * from stat_child_id_device_active where date = '" + date + "') as b on a.app_id = b.app_id and a.child_id=b.child_id and a.date = b.date left join (select * from stat_user_pay_retention_rate_child_id where register_date = '" + date + "') as c on a.app_id=c.app_id and a.child_id=c.child_id and a.date=c.register_date left JOIN (select * from stat_pay_order_child_id where date='" + date + "') as d on a.app_id=d.app_id and a.child_id=d.child_id and a.date = d.date";
        executeSql(childSQL , null);


        String channelIdSQL = "insert into stat_report_forms_day_channel_id (app_id, child_id, channel_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) select a.app_id, a.child_id, a.channel_id, ifnull(b.new_device_count,0), ifnull(a.total_register_count,0), ifnull(a.login_count,0), ifnull(c.register_count,0), ifnull(c.money,0), ifnull(d.pay_user_count,0)-ifnull(c.register_count,0), ifnull(d.pay_total_amount,0)-ifnull(c.money,0), ifnull(d.pay_user_count,0), ifnull(d.pay_total_amount,0), a.date from (select date, app_id, child_id, channel_id, login_count, total_register_count from stat_user_register_login_channel_id_" + year + " where date='" + date + "') as a inner join (select * from stat_channel_id_device_active_" + year + " where date='" + date + "') as b on a.app_id = b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.date = b.date left join (select * from stat_user_pay_retention_rate_channel_id where register_date='" + date + "') as c on a.app_id=c.app_id and a.child_id=c.child_id and a.channel_id=c.channel_id and a.date=c.register_date left JOIN (select * from stat_pay_order_channel_id where date='" + date + "') as d on a.app_id=d.app_id and a.child_id=d.child_id and a.channel_id=d.channel_id and a.date=d.date";
        executeSql(channelIdSQL , null);


        String appChannelIdSQL = "insert into stat_report_forms_day_app_channel_id (app_id, child_id, channel_id, app_channel_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) select a.app_id, a.child_id, a.channel_id, a.app_channel_id, ifnull(b.new_device_count,0), ifnull(a.total_register_count,0), ifnull(a.login_count,0), ifnull(c.register_count,0), ifnull(c.money,0), ifnull(d.pay_user_count,0)-ifnull(c.register_count,0), ifnull(d.pay_total_amount,0)-ifnull(c.money,0), ifnull(d.pay_user_count,0), ifnull(d.pay_total_amount,0), a.date from (select date, app_id, child_id, channel_id, app_channel_id, login_count, total_register_count from stat_user_register_login_app_channel_id_" + year + " where date='" + date + "') as a inner join (select * from stat_app_channel_id_device_active_" + year + " where date = '" + date + "') as b on a.app_id = b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.app_channel_id=b.app_channel_id and a.date = b.date  left join (select * from stat_user_pay_retention_rate_app_channel_id where register_date = '" + date + "') as c on a.app_id=c.app_id and a.child_id=c.child_id and a.channel_id=c.channel_id and  a.app_channel_id=c.app_channel_id and a.date=c.register_date left JOIN (select * from stat_pay_order_app_channel_id where date='" + date + "') as d on a.app_id=d.app_id and a.child_id=d.child_id and a.channel_id=d.channel_id and a.app_channel_id=d.app_channel_id and a.date=d.date";
        executeSql(appChannelIdSQL , null);

        String packageIdSQL = "insert into stat_report_forms_day_package_id (app_id, child_id, channel_id, app_channel_id, package_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) select a.app_id, a.child_id, a.channel_id, a.app_channel_id, a.package_id, ifnull(b.new_device_count,0), ifnull(a.total_register_count,0), ifnull(a.login_count,0), ifnull(c.register_count,0), ifnull(c.money,0), ifnull(d.pay_user_count,0)-ifnull(c.register_count,0), ifnull(d.pay_total_amount,0)-ifnull(c.money,0), ifnull(d.pay_user_count,0), ifnull(d.pay_total_amount,0), a.date from  (select date, app_id, child_id, channel_id, app_channel_id, package_id, login_count, total_register_count from stat_user_register_login_package_id_" + year + " where date = '" + date + "') as a inner join (select * from stat_package_id_device_active_" + year + " where date = '" + date + "') as b  on a.app_id = b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.app_channel_id=b.app_channel_id and a.package_id=b.package_id and a.date=b.date left join (select * from stat_user_pay_retention_rate_package_id where register_date = '" + date + "') as c on a.app_id=c.app_id and a.child_id=c.child_id and a.channel_id=c.channel_id and a.app_channel_id=c.app_channel_id and a.package_id=c.package_id and a.date=c.register_date left JOIN (select * from stat_pay_order_package_id where date='" + date + "') as d on a.app_id=d.app_id and a.child_id=d.child_id and  a.channel_id=d.channel_id and a.app_channel_id=d.app_channel_id and a.package_id=d.package_id and a.date = d.date";
        executeSql(packageIdSQL , null);

    }

    private void dayReportRetention(String nowDate){

        int[] days = new int[]{1, 2, 6, 14};
        for (int day:days) {
            try {
                String date = DateUtils.dateAgo(nowDate , day);
                String col = map.get(day);

                String appIdSQL = "update stat_report_forms_day_app_id as a , stat_user_retention_rate_app_id as b set a.%s=b.%s where a.app_id=b.app_id and a.date = b.register_date and a.date='%s'";
                executeSql(String.format(appIdSQL, col, col, date) , null);

                String childIDSQL = "update stat_report_forms_day_child_id as a, stat_user_retention_rate_child_id as b set a.%s=b.%s where a.app_id=b.app_id and a.child_id=b.child_id and a.date=b.register_date and a.date='%s' ";
                executeSql(String.format(childIDSQL, col , col , date), null);

                String channelIDSQL = "update stat_report_forms_day_channel_id as a, stat_user_retention_rate_channel_id as b set a.%s=b.%s where a.app_id=b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.date=b.register_date and a.date='%s' ";
                executeSql(String.format(channelIDSQL , col , col , date), null);

                String appChannelIDSQL = "update stat_report_forms_day_app_channel_id as a, stat_user_retention_rate_app_channel_id as b set a.%s=b.%s where a.app_id=b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.app_channel_id=b.app_channel_id and a.date=b.register_date and a.date='%s' ";
                executeSql(String.format(appChannelIDSQL , col , col , date), null);

                String packageIDSQL = "update stat_report_forms_day_package_id as a, stat_user_retention_rate_package_id as b set a.%s=b.%s where a.app_id=b.app_id and a.child_id=b.child_id and a.channel_id=b.channel_id and a.app_channel_id=b.app_channel_id and a.package_id=b.package_id and a.date=b.register_date and a.date='%s' ";
                executeSql(String.format(packageIDSQL , col , col , date), null);

            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }


    public static void main(String[] args) throws Exception {
        DayReport dayReport = new DayReport();

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date nowDate = new Date();
        String date = DateUtils.dateAgo(sdf.format(nowDate), 2);
        dayReport.init(date);
        dayReport.dayReportRetention(date);

//        List<String> list = DateUtils.getBetweenDates("2018-07-20" , "2018-08-19" , "yyyy-MM-dd");
//        for (String s : list) {
//            dayReport.init(s);
//            dayReport.dayReportRetention(s);
//        }
    }
}
