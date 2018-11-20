package com.jiuxiu.yxstat.service.pay;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.payorder.JedisPayOrderKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.service.deviceinstall.AndroidActivationDataServiceNew;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IDEA by chouFy on 2018/7/9.
 *
 * @author ZhouFy
 */
public class PayStatisticsDataService implements Serializable {

    private static PayStatisticsDataService payStatisticsDataService = new PayStatisticsDataService();

    private PayStatisticsDataService() {
    }

    public static PayStatisticsDataService getInstance() {
        return payStatisticsDataService;
    }

    Object readResolve() {
        return payStatisticsDataService;
    }

    public void payOrderData(JavaRDD<JSONObject> javaRDD) {

        javaRDD.foreach(json -> {
            int appID = json.getInt("appid");
            int childID = json.getInt("child_id");
            int channelID = json.getInt("channel_id");
            int appChannelID = json.getInt("app_channel_id");
            int packageID = json.getInt("package_id");
            String uid = json.getString("uid");
            long ts = json.getLong("ts");
            String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);

            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.APP_ID_PAY_ORDER + appID, ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME,
                    uid + ":" + childID);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.CHILD_ID_PAY_ORDER + childID + ":" + appID,
                    ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME, uid);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.CHANNEL_ID + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME, uid);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.APP_CHANNEL_ID + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME, uid);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.PACKAGE_ID + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME, uid);
        });

        javaRDD.mapToPair(json -> {
            int appID = json.getInt("appid");
            int childID = json.getInt("child_id");
            int channelID = json.getInt("channel_id");
            int appChannelID = json.getInt("app_channel_id");
            int packageID = json.getInt("package_id");
            StringBuilder key = new StringBuilder();
            key.append(appID);
            key.append("#");
            key.append(childID);
            key.append("#");
            key.append(channelID);
            key.append("#");
            key.append(appChannelID);
            key.append("#");
            key.append(packageID);
            key.append("#");
            long ts = json.getLong("ts");
            String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
            key.append(date);
            Map<String, Integer> map = new HashMap<>(4);
            map.put("payOrderCount", 1);
            map.put("payTotalAmount", json.getInt("money"));
            System.out.println("=======" + key.toString());
            return new Tuple2<>(key.toString(), map);
        }).reduceByKey((stringIntegerMap , stringIntegerMap2) -> {
            Map<String, Integer> map = new HashMap<>(4);
            map.put("payOrderCount", stringIntegerMap.get("payOrderCount") + stringIntegerMap2.get("payOrderCount"));
            map.put("payTotalAmount", stringIntegerMap.get("payTotalAmount") + stringIntegerMap2.get("payTotalAmount"));
            return map;
        }).foreach(tuple2 -> {

            String[] keys = tuple2._1.split("#");
            int keyLength = 6;

            System.out.println("=======" + keys.length + " = " + tuple2._1);

            if(keyLength == keys.length){
                String appID = keys[0];
                String childID = keys[1];
                String channelID = keys[2];
                String appChannelID = keys[3];
                String packageID = keys[4];
                String date = keys[5];
                int payOrderCount = tuple2._2.get("payOrderCount");
                int payTotalAmount = tuple2._2.get("payTotalAmount");
                SavePayOrderDataUtils.savePayOrderData(date,Integer.parseInt(appID), Integer.parseInt(childID), Integer.parseInt(channelID),
                        Integer.parseInt(appChannelID) , Integer.parseInt(packageID) , payOrderCount, payTotalAmount);
            }
        });
    }


    public static void main(String[] args) throws  Exception {

        List<String> list = Files.readAllLines(Paths.get("/opt/yxtest/sparktest/info.log2018-11-15"));


        List<JSONObject> l = new ArrayList<>();


        for (String pay : list) {
            if(pay.contains("package_id\":250")) {
                JSONObject json = JSONObject.fromObject(pay);
                l.add(json);
            }
        }

        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<JSONObject> javaRDD = sc.parallelize(l);

        PayStatisticsDataService android = new PayStatisticsDataService();
        android.payOrderData(javaRDD);


    }
}
