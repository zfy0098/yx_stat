package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.StatActivationStatisticsDao;
import com.jiuxiu.yxstat.dao.stat.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.JedisAppChannelIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisAppIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisChannelIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisChildIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPackageIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.search.SearchResponse;

import java.io.Serializable;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class AndroidActivationDataService implements Serializable{

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();


    public static void androidActivationData(JavaRDD<JSONObject> android) {

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        android.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                String imei = json.getString("imei");
                SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei, appID, childID);
                if (searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null || searchResponse.getHits().getHits().length == 0) {
                    // 没有激活
                    deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, appID);
                    //  保存 child id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                    //  保存 app id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
                    //  保存 app channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                    //  保存 channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                    //  保存 package id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                }
                // child id 启动数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" + imei);
                if (value == null) {
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" + imei, json.toString(), 0);
                }
                //  app channel id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" + imei);
                if (value == null) {
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" + imei, json.toString(), 0);
                }
                //  app id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appID + ":" + imei);
                if (value == null) {
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appID + ":" + imei, json.toString(), 0);
                }
                //  channel id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" + imei);
                if (value == null) {
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" + imei, json.toString(), 0);
                }
                //  package id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" + imei);
                if (value == null) {
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" + imei, json.toString(), 0);
                }
            }
        });
        android.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                /*
                 *   保存 新增设备数
                 */
                // 保存 child id 新增设备数 (激活数量)
                String childIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                if (childIDNewDeviceCount != null) {
                    statChildDeviceActiveDao.saveChildIdNewDeviceCount(new Object[]{childID, childIDNewDeviceCount, childIDNewDeviceCount});
                }
                // 保存 app Channel id 新增设备数
                String appChannelIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                if (appChannelIDNewDeviceCount != null) {
                    statAppChannelIdDeviceActiveDao.saveAppChannelIdNewDeviceCount(new Object[]{appChannelID, appChannelIDNewDeviceCount, appChannelIDNewDeviceCount});
                }
                // 保存 app id 新增设备数
                String appIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
                if (appIDNewDeviceCount != null) {
                    statAppIdDeviceActiveDao.saveAppIdNewDeviceCount(new Object[]{appID, appIDNewDeviceCount, appIDNewDeviceCount});
                }
                // 保存 channel id 新增设备数
                String channelIDNewDevice = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                if (channelIDNewDevice != null) {
                    statChannelIdDeviceActiveDao.saveChannelIdNewDeviceCount(new Object[]{channelID, channelIDNewDevice, channelIDNewDevice});
                }
                // 保存 package id 新增设备数
                String packageIDNewDevice = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                if (packageIDNewDevice != null) {
                    statPackageIdDeviceActiveDao.savePackageIdNewDeviceCount(new Object[]{packageID, packageIDNewDevice, packageIDNewDevice});
                }

                /*
                 *  保存启动设备数
                 */
                // package id 启动设备数
                String packageIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                if (packageIDStartupCount != null) {
                    statPackageIdDeviceActiveDao.savePackageIdDeviceStartUpCount(new Object[]{packageID, packageIDStartupCount, packageIDStartupCount});
                }
                // app channel id 启动设备数
                String appChannelIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                if (appChannelIDStartupCount != null) {
                    statAppChannelIdDeviceActiveDao.saveAppChannelIdDeviceStartUpCount(new Object[]{appChannelID, appChannelIDStartupCount, appChannelIDStartupCount});
                }
                // appid 启动设备数
                String appidStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
                if (appidStartupCount != null) {
                    statAppIdDeviceActiveDao.saveAppIdDeviceStartUpCount(new Object[]{appID, appidStartupCount, appidStartupCount});
                }
                //  channel  id  启动设备数
                String channelIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                if (channelIDStartupCount != null) {
                    statChannelIdDeviceActiveDao.saveChannelIdDeviceStartUpCount(new Object[]{channelID, channelIDStartupCount, channelIDStartupCount});
                }
                // child id 启动设备数
                String childIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                if (childIDStartupCount != null) {
                    statChildDeviceActiveDao.saveChildIdDeviceStartUpCount(new Object[]{childID, childIDStartupCount, childIDStartupCount});
                }
            }
        });
    }
}
