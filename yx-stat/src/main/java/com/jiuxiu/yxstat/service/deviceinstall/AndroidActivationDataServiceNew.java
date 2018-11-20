package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.deviceinstall.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.service.deviceinstall.channel.ToutiaoChannel;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.elasticsearch.action.search.SearchResponse;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class AndroidActivationDataServiceNew implements Runnable, Serializable {

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();

    /**
     * 天统计DAO类对象
     */
    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();


    private JavaRDD<JSONObject> android;

    AndroidActivationDataServiceNew(JavaRDD<JSONObject> android) {
        this.android = android;
    }


    private ToutiaoChannel toutiaoChannel = ToutiaoChannel.getInstance();


    @Override
    public void run() {
        JavaRDD<JSONObject> androids = android.filter(json -> {
            int appID = json.getInt("appid");
            int childID = json.getInt("child_id");
            int appChannelID = json.getInt("app_channel_id");
            int channelID = json.getInt("channel_id");
            int packageID = json.getInt("package_id");

            String imei = json.getString("imei");

            long second = json.getLong("ts");
            String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
            String toDay = DateUtil.secondToDateString(second, DateUtil.YYYY_MM_DD);

            //  判断 imei 是否激活
            SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei, appID, childID);
            if (searchResponse.getHits().getHits().length == 0) {

                // 没有激活

                //  判断是否为头条渠道   channel = 59
                if (channelID == 59) {
                    toutiaoChannel.getAdClickInfo(json);
                } else {
                    System.out.println("非上报渠道数据：" + json.toString());
                }

                // 没有激活
                deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, appID);
                // 将天数据保存到redis 中
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

                // 将分钟数据保存到redis 中
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

            } else {
                Map<String,Object> map = searchResponse.getHits().getHits()[0].getSource();
                System.out.println("已经激活" + searchResponse.getHits().getHits().length + ", 数据：" + map.toString());
            }

            // 保存启动信息
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

            // 保存分钟数据
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei + ":" + childID);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                    ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

            return true;
        });

        androids.mapToPair(json -> {
            StringBuffer key = new StringBuffer();
            key.append(json.getInt("package_id"));
            key.append("#");
            key.append(json.getInt("child_id"));
            key.append("#");
            key.append(json.getInt("app_channel_id"));
            key.append("#");
            key.append(json.getInt("channel_id"));
            key.append("#");
            key.append(json.getInt("appid"));
            long second = json.getLong("ts");
            String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
            String toDay = DateUtil.secondToDateString(second, DateUtil.YYYY_MM_DD);
            key.append("#");
            key.append(time);
            key.append("#");
            key.append(toDay);
            return new Tuple2<>(key.toString(), 1);

        }).reduceByKey((integer, integer2) ->
                integer + integer2
        ).foreach(tuple2 -> {
            String[] keys = tuple2._1.split("#");
            int keyLength = 7;
            if (keys.length == keyLength) {
                String packageID = keys[0];
                String childID = keys[1];
                String appChannelID = keys[2];
                String channelID = keys[3];
                String appid = keys[4];

                String time = keys[5];
                String today = keys[6];

                // 保存天统计数据
                SaveDataUtils.saveDeviceInstallData(today, Integer.parseInt(appid), Integer.parseInt(childID), Integer.parseInt(channelID)
                        , Integer.parseInt(appChannelID), Integer.parseInt(packageID));

                // 保存分钟统计数据
                SaveMinuteDataUtils.saveMinuteDeviceInstallData(time, Integer.parseInt(appid), Integer.parseInt(childID), Integer.parseInt(channelID)
                        , Integer.parseInt(appChannelID), Integer.parseInt(packageID));

                /*
                 *   保存天统计数据
                 */
                statChildDeviceActiveDao.saveChildIDStartUpCount(today, appid, childID, tuple2._2);
                statAppChannelIdDeviceActiveDao.saveAppChannelIdStartUpCount(today, appid, childID, channelID, appChannelID, tuple2._2);
                statChannelIdDeviceActiveDao.saveChannelIdStartUpCount(today, appid, childID, channelID, tuple2._2);
                statAppIdDeviceActiveDao.saveAppIdStartUpCount(today, appid, tuple2._2);
                statPackageIdDeviceActiveDao.savePackageIdStartUpCount(today, appid, childID, channelID, appChannelID, packageID, tuple2._2);

                /*
                 *  保存分钟统计数据
                 */
                statChildDeviceActiveDao.saveChildIDMinuteStartUpCount(time, appid, childID, tuple2._2);
                statAppChannelIdDeviceActiveDao.saveAppChannelIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, tuple2._2);
                statChannelIdDeviceActiveDao.saveChannelIDMinuteStartUpCount(time, appid, childID, channelID, tuple2._2);
                statAppIdDeviceActiveDao.saveAppIDMinuteStartUpCount(time, appid, tuple2._2);
                statPackageIdDeviceActiveDao.savePackageIDMinuteStartUpCount(time, appid, childID, channelID, appChannelID, packageID, tuple2._2);
            }
        });
    }

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String temp = "{\"device_os_ver\":\"Android 8.0.0\",\"imei\":\"869161022676019\",\"logid\":\"dd83e289652b7922c299eeb28529e9a0\",\"api_ver\":\"1.3.0\",\"app_ver\":\"145\",\"mac\":\"\",\"package_id\":249,\"child_id\":49,\"device_name\":\"MI 5\",\"app_channel_id\":96,\"os\":1,\"sdk_ver\":\"1.3.0\",\"channel_id\":59,\"appid\":10014,\"idfa\":\"\",\"client_ip\":\"103.244.255.230\",\"ts\":1541740615}";
        JSONObject json = JSONObject.fromObject(temp);

        List<JSONObject> list = new ArrayList<>();
        list.add(json);

        JavaRDD<JSONObject> javaRDD = sc.parallelize(list);

        AndroidActivationDataServiceNew android = new AndroidActivationDataServiceNew(javaRDD);
        android.run();
    }
}
