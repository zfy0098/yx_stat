package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.es.deviceinstall.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.search.SearchResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/12.
 *
 * @author ZhouFy
 */
public class AndroidActivationDataService implements Runnable ,  Serializable {

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();

    private JavaRDD<JSONObject> android;

    AndroidActivationDataService(JavaRDD<JSONObject> android) {
        this.android = android;
    }

    @Override
    public void run() {
        android.filter(new Function<JSONObject, Boolean>() {
            Map<String,JSONObject> map = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject json) throws Exception {
                StringBuilder key = new StringBuilder();
                key.append(json.getInt("appid"));
                key.append(json.getInt("child_id"));
                key.append(json.getInt("app_channel_id"));
                key.append(json.getInt("channel_id"));
                key.append(json.getInt("package_id"));
                key.append(json.getString("imei"));
                if(map.get(key.toString()) == null){
                    map.put(key.toString() , json);
                    return true;
                }
                return false;
            }
        }).foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                String imei = json.getString("imei");

                long second = json.getLong("ts");
                String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
                String toDay = DateUtil.secondToDateString(second , DateUtil.YYYY_MM_DD);

                SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei, appID, childID);
                if (searchResponse.getHits().getHits().length == 0) {
                    // 没有激活
                    deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, appID);
                    // 将天数据保存到redis 中
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID ,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , imei + ":" + childID);
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME ,  imei);
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , imei);
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , imei);
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME ,  imei);

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
            }
        });
        android.filter(new Function<JSONObject, Boolean>() {
            Map<String, Object> map = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject json) {
                StringBuilder key = new StringBuilder();
                key.append(json.getInt("package_id"));
                key.append(json.getInt("child_id"));
                key.append(json.getInt("app_channel_id"));
                key.append(json.getInt("channel_id"));
                key.append(json.getInt("appid"));
                long ts = json.getLong("ts");
                String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);
                key.append(date);
                key.append(time);

                if (map.get(key.toString()) == null) {
                    map.put(key.toString(), json);
                    return true;
                }
                return false;
            }
        }).foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                long second = json.getLong("ts");
                String time = DateUtil.getNowFutureWhileMinute(second);
                String toDay = DateUtil.secondToDateString(second , DateUtil.YYYY_MM_DD);

                // 保存天统计数据
                SaveDataUtils.saveDeviceInstallData(toDay, appID, childID, channelID, appChannelID, packageID);

                // 保存分钟统计数据
                SaveMinuteDataUtils.saveMinuteDeviceInstallData(time, appID, childID, channelID, appChannelID, packageID);
            }
        });
    }
}
