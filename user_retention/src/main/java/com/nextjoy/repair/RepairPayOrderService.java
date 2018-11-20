package com.nextjoy.repair;

import com.nextjoy.retention.db.StatDataBase;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;
import java.util.Map;

/**
 * Created with IDEA by ChouFy on 2018/11/16.
 *
 * @author Zhoufy
 */
public class RepairPayOrderService extends StatDataBase implements Serializable {


    private void repairPayOrder(){
        SparkConf sparkConf = new SparkConf().setAppName("RepairPayOrderService").setMaster("spark://172.26.110.193:7077");

        SparkSession sqlContext = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate();

        sqlContext.sql("use yxstat");


        String sql = "select appid, child_id, channel_id, app_channel_id, package_id, count(distinct(uid)) as payusercount, count(1) as count, sum(money) as money, riqi" +
                " from pay_order where riqi between '2018-07-09' and '2018-11-15' and order_type = 1 group by appid, child_id, channel_id, app_channel_id, package_id, riqi";

        Dataset<Row> dataSet = sqlContext.sql(sql);
        for (Row row : dataSet.collectAsList()) {
            int appid = row.getInt(0);
            int childID = row.getInt(1);
            int channelID = row.getInt(2);
            int appChannelID = row.getInt(3);
            int packageID = row.getInt(4);
            long payUserCount = row.getLong(5);
            long payOrderCount = row.getLong(6);
            long payOrderMoney = row.getLong(7);
            String date = row.getString(8);

            savePackageIDPayOrder(date, appid, childID, channelID, appChannelID, packageID, payUserCount, payOrderMoney, payOrderCount);
        }
    }


    private void savePackageIDPayOrder(String date, int appid, int childID, int channelID, int appChannelID, int packageID, long payUserCount, long payTotalAmount, long payOrderCount) {
        String querySQL = "select id from stat_pay_order_package_id where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
        Map<String, Object> map = queryForMap(querySQL, new Object[]{appid, childID, channelID, appChannelID, packageID, date});
        if (map == null || map.isEmpty()) {
            String sql = "insert into stat_pay_order_package_id (app_id, child_id, channel_id, app_channel_id, package_id, pay_user_count, pay_total_amount, pay_order_count, date) values (?,?,?,?,?,?,?,?,?)";
            executeSql(sql, new Object[]{appid, childID, channelID, appChannelID, packageID, payUserCount, payTotalAmount, payOrderCount, date});
        } else {
            String sql = "update stat_pay_order_package_id set pay_user_count = ?, pay_total_amount =  ?, pay_order_count =  ? " +
                    "where app_id = ? and child_id = ? and channel_id = ? and app_channel_id = ? and package_id = ? and date = ?";
            executeSql(sql, new Object[]{payUserCount, payTotalAmount, payOrderCount, appid, childID, channelID, appChannelID, packageID, date});
        }
    }


    public static void main(String[] args) {

        RepairPayOrderService repairPayOrderService = new RepairPayOrderService();
        repairPayOrderService.repairPayOrder();
    }


}
