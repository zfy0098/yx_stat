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
public class DayReportChildIDDao extends StatDataBase {


    private static DayReportChildIDDao dayReportChildIDDao;

    private DayReportChildIDDao(){}

    public static DayReportChildIDDao getInstance(){
        if(dayReportChildIDDao == null){
            dayReportChildIDDao = new DayReportChildIDDao();
        }
        return dayReportChildIDDao;
    }




    public void saveChildIDDayReport(String date){

        Map<String, Map<String,Object>> data = new HashMap<>(16);

        String childIDNewDeviceCount = "select * from stat_child_id_device_active where date = ?";
        List<Map<String,Object>> deviceList = queryForList(childIDNewDeviceCount, new Object[]{date});
        // 遍历新增设备数
        for (Map<String,Object> device : deviceList) {
            int appid = Integer.parseInt(device.get("app_id").toString());
            int childID = Integer.parseInt(device.get("child_id").toString());

            String key = appid + "#" + childID;

            Map<String,Object> deviceMap = data.get(key);
            if (deviceMap  == null){
                deviceMap = new HashMap<>();
                deviceMap.put("app_id", appid);
                deviceMap.put("child_id", childID);
                deviceMap.put("new_device_count" , device.get("new_device_count"));

                data.put(key, deviceMap);
            } else {
                deviceMap.put("new_device_count" , device.get("new_device_count"));
            }
        }

        String childIDRegisterCount = "select * from stat_user_register_login_child_id where date = ?";
        List<Map<String,Object>> registerList = queryForList(childIDRegisterCount, new Object[]{date});
        // 遍历注册数
        for (Map<String,Object> register : registerList) {
            int appid = Integer.parseInt(register.get("app_id").toString());
            int childID = Integer.parseInt(register.get("child_id").toString());

            String key = appid + "#" + childID;

            Map<String, Object> registerMap = data.get(key);
            if(registerMap == null){
                registerMap = new HashMap<>();
                registerMap.put("app_id", appid);
                registerMap.put("child_id", childID);
                registerMap.put("register_count", register.get("total_register_count"));
                registerMap.put("login_count", register.get("login_count"));

                data.put(key, registerMap);
            } else {
                registerMap.put("register_count", register.get("total_register_count"));
                registerMap.put("login_count", register.get("login_count"));
            }
        }

        // 付费注册
        String childIDPayUserRegister = "select * from stat_user_pay_retention_rate_child_id where register_date = ?";
        List<Map<String,Object>> payUserList = queryForList(childIDPayUserRegister, new Object[]{date});
        for (Map<String,Object> payUser : payUserList) {
            int appid = Integer.parseInt(payUser.get("app_id").toString());
            int childID = Integer.parseInt(payUser.get("child_id").toString());

            String key = appid + "#" + childID;

            Map<String, Object> payUserMap = data.get(key);
            if(payUserMap == null){
                payUserMap = new HashMap<>();
                payUserMap.put("app_id", appid);
                payUserMap.put("child_id", childID);
                payUserMap.put("new_pay_user_count", payUser.get("register_pay_count"));
                payUserMap.put("new_pay_money", payUser.get("money"));

                data.put(key, payUserMap);
            } else {
                payUserMap.put("new_pay_user_count", payUser.get("register_pay_count"));
                payUserMap.put("new_pay_money", payUser.get("money"));
            }
        }

        //充值
        String appIDPayOrder = "select * from stat_pay_order_child_id WHERE date = ?";
        List<Map<String,Object>> payOrderList = queryForList(appIDPayOrder, new Object[]{date});
        for (Map<String,Object> payOrder: payOrderList) {
            int appid = Integer.parseInt(payOrder.get("app_id").toString());
            int childID = Integer.parseInt(payOrder.get("child_id").toString());

            String key = appid + "#" + childID;

            Map<String, Object> payOrderMap = data.get(key);
            if(payOrderMap == null){
                payOrderMap = new HashMap<>();
                payOrderMap.put("app_id", appid);
                payOrderMap.put("child_id", childID);

                int payUserCount = Integer.parseInt(payOrder.getOrDefault("pay_user_count", "0").toString());
                int payTotalAmount = Integer.parseInt(payOrder.getOrDefault("pay_total_amount", "0").toString());
                payOrderMap.put("total_pay_user_count", payUserCount);
                payOrderMap.put("total_pay_money", payTotalAmount);

                int newPayUserCount = Integer.parseInt(payOrderMap.getOrDefault("new_pay_user_count", "0").toString());
                int newPayMoney = Integer.parseInt(payOrderMap.getOrDefault("new_pay_money","0").toString());

                payOrderMap.put("old_pay_user_count" , payUserCount - newPayUserCount);
                payOrderMap.put("old_pay_money", payTotalAmount - newPayMoney);

                data.put(key, payOrder);

            } else {
                int payUserCount = Integer.parseInt(payOrder.getOrDefault("pay_user_count", "0").toString());
                int payTotalAmount = Integer.parseInt(payOrder.getOrDefault("pay_total_amount", "0").toString());
                payOrderMap.put("total_pay_user_count", payUserCount);
                payOrderMap.put("total_pay_money", payTotalAmount);

                int newPayUserCount = Integer.parseInt(payOrderMap.getOrDefault("new_pay_user_count", "0").toString());
                int newPayMoney = Integer.parseInt(payOrderMap.getOrDefault("new_pay_money","0").toString());

                payOrderMap.put("old_pay_user_count" , payUserCount - newPayUserCount);
                payOrderMap.put("old_pay_money", payTotalAmount - newPayMoney);
            }
        }

        String childIDDayReport = "select * from stat_report_forms_day_child_id where app_id = ? and child_id = ? and date = ?";

        List<Object[]> params = new ArrayList<>();
        for (Map.Entry<String, Map<String,Object>> appIDData : data.entrySet()) {
            Map<String,Object> map1 = appIDData.getValue();

            int appid = Integer.parseInt(map1.get("app_id").toString());
            int childID = Integer.parseInt(map1.get("child_id").toString());


            Map<String, Object> map = queryForMap(childIDDayReport, new Object[]{appid, childID, date});
            if(map == null || map.isEmpty()){
                params.add(new Object[]{appid, childID, map1.getOrDefault("new_device_count" , 0), map1.getOrDefault("register_count" , 0)
                        , map1.getOrDefault("login_count" , 0), map1.getOrDefault("new_pay_user_count" , 0), map1.getOrDefault("new_pay_money" , 0)
                        , map1.getOrDefault("old_pay_user_count", 0), map1.getOrDefault("old_pay_money" , 0), map1.getOrDefault("total_pay_user_count", 0)
                        , map1.getOrDefault("total_pay_money" , 0) , date});
            } else {
                String update = "update stat_report_forms_day_child_id set new_device_count = ?, register_count = ?, login_count = ?, new_pay_user_count = ?, new_pay_money = ?, old_pay_user_count = ?, old_pay_money = ?, total_pay_user_count = ?, total_pay_money = ? where app_id = ? and child_id = ? and date = ? ";
                executeSql(update, new Object[]{map1.getOrDefault("new_device_count" , 0), map1.getOrDefault("register_count" , 0)
                        , map1.getOrDefault("login_count" , 0), map1.getOrDefault("new_pay_user_count" , 0), map1.getOrDefault("new_pay_money" , 0)
                        , map1.getOrDefault("old_pay_user_count", 0), map1.getOrDefault("old_pay_money" , 0), map1.getOrDefault("total_pay_user_count", 0)
                        , map1.getOrDefault("total_pay_money" , 0) , appid, childID, date});
            }
        }

        String saveChildIDDayReport = "insert into stat_report_forms_day_child_id (app_id, child_id, new_device_count, register_count, login_count, new_pay_user_count, new_pay_money, old_pay_user_count, old_pay_money, total_pay_user_count, total_pay_money, date) value (?,?,?,?,?,?,?,?,?,?,?,?)";
        executeBatchSql(saveChildIDDayReport, params);
    }
}
