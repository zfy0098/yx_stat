package com.nextjoy.report.dao;

import com.nextjoy.retention.db.StatDataBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/19.
 *
 * @author Zhoufy
 */
public class DayReportPackageIDDao extends StatDataBase {

    private static DayReportPackageIDDao dayReportPackageIDDao;

    private DayReportPackageIDDao() {
    }


    public static DayReportPackageIDDao getInstance() {
        if (dayReportPackageIDDao == null) {
            dayReportPackageIDDao = new DayReportPackageIDDao();
        }
        return dayReportPackageIDDao;
    }


    public void savepackageIDDayReport(String date) {

        String year = date.split("-")[0];

        Map<String, Map<String, Object>> data = new HashMap<>(16);

        String packageIDNewDeviceCount = "select * from stat_package_id_device_active_" + year + "  where date = ?";
        List<Map<String, Object>> deviceList = queryForList(packageIDNewDeviceCount, new Object[]{date});
        // 遍历新增设备数
        for (Map<String, Object> device : deviceList) {
            int appid = Integer.parseInt(device.get("app_id").toString());
            int childID = Integer.parseInt(device.get("child_id").toString());
            int channelID = Integer.parseInt(device.get("channel_id").toString());
            int appChannelID = Integer.parseInt(device.get("app_channel_id").toString());
            int packageID = Integer.parseInt(device.get("package_id").toString());

            String key = appid + "#" + childID + "#" + channelID + "#" + appChannelID + "#" + packageID;

            Map<String, Object> deviceMap = data.get(key);
            if (deviceMap == null) {
                deviceMap = new HashMap<>();
                deviceMap.put("app_id", appid);
                deviceMap.put("child_id", childID);
                deviceMap.put("channel_id", channelID);
                deviceMap.put("app_channel_id", appChannelID);
                deviceMap.put("package_id", packageID);
                deviceMap.put("new_device_count", device.get("new_device_count"));

                data.put(key, deviceMap);
            } else {
                deviceMap.put("new_device_count", device.get("new_device_count"));
            }
        }

        String packageIDRegisterCount = "select * from stat_user_register_login_package_id_" + year + " where date = ?";
        List<Map<String, Object>> registerList = queryForList(packageIDRegisterCount, new Object[]{date});
        // 遍历注册数
        for (Map<String, Object> register : registerList) {
            int appid = Integer.parseInt(register.get("app_id").toString());
            int childID = Integer.parseInt(register.get("child_id").toString());
            int channelID = Integer.parseInt(register.get("channel_id").toString());
            int appChannelID = Integer.parseInt(register.get("app_channel_id").toString());
            int packageID = Integer.parseInt(register.get("package_id").toString());

            String key = appid + "#" + childID + "#" + channelID + "#" + appChannelID + "#" + packageID;

            Map<String, Object> registerMap = data.get(key);
            if (registerMap == null) {
                registerMap = new HashMap<>();
                registerMap.put("app_id", appid);
                registerMap.put("child_id", childID);
                registerMap.put("channel_id", channelID);
                registerMap.put("app_channel_id", appChannelID);
                registerMap.put("package_id", packageID);
                registerMap.put("register_count", register.get("total_register_count"));
                registerMap.put("login_count", register.get("login_count"));

                data.put(key, registerMap);
            } else {
                registerMap.put("register_count", register.get("total_register_count"));
                registerMap.put("login_count", register.get("login_count"));
            }
        }

        // 付费注册
        String packageIDPayUserRegister = "select * from stat_user_pay_retention_rate_package_id where register_date = ?";
        List<Map<String, Object>> payUserList = queryForList(packageIDPayUserRegister, new Object[]{date});
        for (Map<String, Object> payUser : payUserList) {
            int appid = Integer.parseInt(payUser.get("app_id").toString());
            int childID = Integer.parseInt(payUser.get("child_id").toString());
            int channelID = Integer.parseInt(payUser.get("channel_id").toString());
            int appChannelID = Integer.parseInt(payUser.get("app_channel_id").toString());
            int packageID = Integer.parseInt(payUser.get("package_id").toString());

            String key = appid + "#" + childID + "#" + channelID + "#" + appChannelID + "#" + packageID;

            Map<String, Object> payUserMap = data.get(key);
            if (payUserMap == null) {
                payUserMap = new HashMap<>();
                payUserMap.put("app_id", appid);
                payUserMap.put("child_id", childID);
                payUserMap.put("channel_id", channelID);
                payUserMap.put("app_channel_id", appChannelID);
                payUserMap.put("package_id", packageID);
                payUserMap.put("new_pay_user_count", payUser.get("register_pay_count"));
                payUserMap.put("new_pay_money", payUser.get("money"));

                data.put(key, payUserMap);
            } else {
                payUserMap.put("new_pay_user_count", payUser.get("register_pay_count"));
                payUserMap.put("new_pay_money", payUser.get("money"));
            }
        }

        //充值
        String packageIDPayOrder = "select * from stat_pay_order_package_id WHERE date = ?";
        List<Map<String, Object>> payOrderList = queryForList(packageIDPayOrder, new Object[]{date});
        for (Map<String, Object> payOrder : payOrderList) {
            int appid = Integer.parseInt(payOrder.get("app_id").toString());
            int childID = Integer.parseInt(payOrder.get("child_id").toString());
            int channelID = Integer.parseInt(payOrder.get("channel_id").toString());
            int appChannelID = Integer.parseInt(payOrder.get("app_channel_id").toString());
            int packageID = Integer.parseInt(payOrder.get("package_id").toString());

            String key = appid + "#" + childID + "#" + channelID + "#" + appChannelID + "#" + packageID;

            Map<String, Object> payOrderMap = data.get(key);
            if (payOrderMap == null) {
                payOrderMap = new HashMap<>();
                payOrderMap.put("app_id", appid);
                payOrderMap.put("child_id", childID);
                payOrderMap.put("channel_id", channelID);
                payOrderMap.put("app_channel_id", appChannelID);
                payOrderMap.put("package_id", packageID);

                int payUserCount = Integer.parseInt(payOrder.getOrDefault("pay_user_count", "0").toString());
                int payTotalAmount = Integer.parseInt(payOrder.getOrDefault("pay_total_amount", "0").toString());
                payOrderMap.put("total_pay_user_count", payUserCount);
                payOrderMap.put("total_pay_money", payTotalAmount);

                int newPayUserCount = Integer.parseInt(payOrderMap.getOrDefault("new_pay_user_count", "0").toString());
                int newPayMoney = Integer.parseInt(payOrderMap.getOrDefault("new_pay_money", "0").toString());

                payOrderMap.put("old_pay_user_count", payUserCount - newPayUserCount);
                payOrderMap.put("old_pay_money", payTotalAmount - newPayMoney);

                data.put(key, payOrderMap);

            } else {

                int payUserCount = Integer.parseInt(payOrder.getOrDefault("pay_user_count", "0").toString());
                int payTotalAmount = Integer.parseInt(payOrder.getOrDefault("pay_total_amount", "0").toString());
                payOrderMap.put("total_pay_user_count", payUserCount);
                payOrderMap.put("total_pay_money", payTotalAmount);

                int newPayUserCount = Integer.parseInt(payOrderMap.getOrDefault("new_pay_user_count", "0").toString());
                int newPayMoney = Integer.parseInt(payOrderMap.getOrDefault("new_pay_money", "0").toString());

                payOrderMap.put("old_pay_user_count", payUserCount - newPayUserCount);
                payOrderMap.put("old_pay_money", payTotalAmount - newPayMoney);

            }
        }

        String packageIDDayReport = "select * from stat_report_forms_day_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";

        List<Object[]> params = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> appIDData : data.entrySet()) {
            Map<String, Object> map1 = appIDData.getValue();

            int appid = Integer.parseInt(map1.get("app_id").toString());
            int childID = Integer.parseInt(map1.get("child_id").toString());
            int channelID = Integer.parseInt(map1.get("channel_id").toString());
            int appChannelID = Integer.parseInt(map1.get("app_channel_id").toString());
            int packageID = Integer.parseInt(map1.get("package_id").toString());


            Map<String, Object> map = queryForMap(packageIDDayReport, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
            if (map == null || map.isEmpty()) {
                params.add(new Object[]{appid, childID, channelID, appChannelID, packageID, map1.getOrDefault("new_device_count", 0), map1.getOrDefault("register_count", 0)
                        , map1.getOrDefault("login_count", 0), map1.getOrDefault("new_pay_user_count", 0), map1.getOrDefault("new_pay_money", 0)
                        , map1.getOrDefault("old_pay_user_count", 0), map1.getOrDefault("old_pay_money", 0), map1.getOrDefault("total_pay_user_count", 0)
                        , map1.getOrDefault("total_pay_money", 0), date});
            } else {
                String update = "update stat_report_forms_day_package_id set new_device_count = ?, register_count = ?, login_count = ?, new_pay_user_count = ?, new_pay_money = ?, old_pay_user_count = ?, old_pay_money = ?, total_pay_user_count = ?, total_pay_money = ? where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ? ";
                executeSql(update, new Object[]{map1.getOrDefault("new_device_count", 0), map1.getOrDefault("register_count", 0)
                        , map1.getOrDefault("login_count", 0), map1.getOrDefault("new_pay_user_count", 0), map1.getOrDefault("new_pay_money", 0)
                        , map1.getOrDefault("old_pay_user_count", 0), map1.getOrDefault("old_pay_money", 0), map1.getOrDefault("total_pay_user_count", 0)
                        , map1.getOrDefault("total_pay_money", 0), appid, childID, channelID, appChannelID, packageID, date});
            }
        }

        String saveAppIDDayReport = "insert into stat_report_forms_day_package_id (app_id, child_id, channel_id, app_channel_id, package_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) value (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
        executeBatchSql(saveAppIDDayReport, params);
    }
}
