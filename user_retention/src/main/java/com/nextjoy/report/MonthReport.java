package com.nextjoy.report;

import com.nextjoy.retention.db.StatDataBase;
import com.nextjoy.retention.utils.DateUtils;

import java.util.Calendar;

/**
 * 月报数据统计
 * <p>
 * Created with IDEA by hadoop on 2018/10/8.
 *
 * @author Choufy
 */
public class MonthReport extends StatDataBase {

    private void init(String month) {

        String monthReportAppId = "insert into stat_report_forms_month_app_id ( app_id, new_device_count, register_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, month )" +
                " select app_id , sum(new_device_count) , sum(register_count) , sum(new_pay_user_count) , sum(new_pay_money) , sum(old_pay_user_count), SUM(old_pay_money) , sum(total_pay_user_count) , sum(total_pay_money) , '" + month + "' " +
                " from stat_report_forms_day_app_id where left(date , 7) = '" + month + "' GROUP BY app_id";

        String monthReportChildID = "insert into stat_report_forms_month_child_id ( app_id, child_id, new_device_count, register_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, month )" +
                "select app_id , child_id , sum(new_device_count) , sum(register_count) , sum(new_pay_user_count) , sum(new_pay_money) , sum(old_pay_user_count), SUM(old_pay_money) , sum(total_pay_user_count) , sum(total_pay_money) ,  '" + month + "' " +
                "from stat_report_forms_day_child_id where left(date , 7) = '" + month + "' GROUP BY app_id , child_id";

        String monthReportChannelID = "insert into stat_report_forms_month_channel_id ( app_id, child_id, channel_id, new_device_count, register_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, month )" +
                "select app_id , child_id , channel_id, sum(new_device_count) , sum(register_count) , sum(new_pay_user_count) , sum(new_pay_money) , sum(old_pay_user_count), SUM(old_pay_money) , sum(total_pay_user_count) , sum(total_pay_money) ,  '" + month + "' " +
                "from stat_report_forms_day_channel_id where left(date , 7) = '" + month + "' GROUP BY app_id , child_id, channel_id";

        String monthReportAppChannelID = "insert into stat_report_forms_month_app_channel_id ( app_id, child_id, channel_id, app_channel_id, new_device_count, register_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, month )" +
                "select app_id , child_id , channel_id, app_channel_id, sum(new_device_count) , sum(register_count) , sum(new_pay_user_count) , sum(new_pay_money) , sum(old_pay_user_count), SUM(old_pay_money) , sum(total_pay_user_count) , sum(total_pay_money) ,  '" + month + "' " +
                "from stat_report_forms_day_app_channel_id where left(date , 7) = '" + month + "' GROUP BY app_id , child_id, channel_id, app_channel_id";

        String monthReportPackageID = "insert into stat_report_forms_month_package_id ( app_id, child_id, channel_id, app_channel_id, package_id, new_device_count, register_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, month )" +
                "select app_id , child_id , channel_id, app_channel_id, package_id, sum(new_device_count) , sum(register_count) , sum(new_pay_user_count) , sum(new_pay_money) , sum(old_pay_user_count), SUM(old_pay_money) , sum(total_pay_user_count) , sum(total_pay_money) ,  '" + month + "' " +
                "from stat_report_forms_day_package_id where left(date , 7) = '" + month + "' GROUP BY app_id , child_id, channel_id, app_channel_id, package_id";

        executeBatchSql(new String[]{monthReportAppId, monthReportChildID, monthReportChannelID, monthReportAppChannelID, monthReportPackageID});
    }

    public static void main(String[] args) {
        String month;
        if (args.length > 0) {
            month = args[0];
        } else {
            Calendar calendar = Calendar.getInstance();
            calendar.add(Calendar.MONTH, -1);
            month = DateUtils.getData(calendar.getTime(), "yyyy-MM");

            System.out.println(calendar.get(Calendar.MONTH) + 1);
            System.out.println(month);
            System.out.println(calendar.get(Calendar.HOUR_OF_DAY));
        }

        MonthReport monthReport = new MonthReport();
        monthReport.init(month);
    }
}
