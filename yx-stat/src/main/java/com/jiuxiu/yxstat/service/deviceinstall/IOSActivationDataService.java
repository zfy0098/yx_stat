package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.es.deviceinstall.DeviceActivationStatisticsESStorage;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
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
public class IOSActivationDataService implements Runnable, Serializable {

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();

    private JavaRDD<JSONObject> ios;

    public IOSActivationDataService(JavaRDD<JSONObject> ios) {
        this.ios = ios;
    }

    @Override
    public void run() {
        ios.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> map = new HashMap<>();

            @Override
            public Boolean call(JSONObject json) throws Exception {
                StringBuilder key = new StringBuilder();
                key.append(json.getInt("appid"));
                key.append(json.getInt("child_id"));
                key.append(json.getInt("app_channel_id"));
                key.append(json.getInt("channel_id"));
                key.append(json.getInt("package_id"));
                String id = json.getString("imei");
                if (StringUtils.isEmpty(id)) {
                    id = json.getString("idfa");
                }
                if (StringUtils.isEmpty(id)) {
                    String deviceName = json.getString("device_name");
                    String deviceOSVer = json.getString("device_os_ver");
                    String ip = json.getString("client_ip");
                    id = deviceName + ":" + deviceOSVer + ":" + ip;
                }
                key.append(id);
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
                String imei = json.getString("imei");
                String idfa = json.getString("idfa");

                long second = json.getLong("ts");
                String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
                String toDay = DateUtil.secondToDateString(second , DateUtil.YYYY_MM_DD);


                boolean flag = false;

                if (StringUtils.isEmpty(imei)) {
                    if (StringUtils.isEmpty(idfa)) {
                        String deviceName = json.getString("device_name");
                        String deviceOSVer = json.getString("device_os_ver");
                        String ip = json.getString("client_ip");

                        SearchResponse searchResponse = deviceActivationStatisticsESStorage.iosSpecialSearchForAppID(ip, deviceName, deviceOSVer, appID, childID);
                        if (searchResponse.getHits().getHits().length < ServiceConstant.ACTIVE_COUNT) {
                            imei = deviceName + ":" + deviceOSVer + ":" + ip + ":" + searchResponse.getHits().getHits().length;
                            flag = true;
                        }
                    } else {
                        SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForIdfa(idfa, appID, childID);
                        if (searchResponse.getHits().getHits().length == 0) {
                            imei = idfa;
                            flag = true;
                        }
                    }
                } else {
                    SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei, appID, childID);
                    if (searchResponse.getHits().getHits().length == 0) {
                        flag = true;
                    }
                }
                if (flag) {
                    // 没有激活
                    deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json, appID);
                    //  保存 app id 激活数
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


                    //  保存 app id 激活数
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

                boolean startupDeviceFlag = false;
                //  启动设备数
                if (StringUtils.isEmpty(imei)) {
                    if (StringUtils.isEmpty(idfa)) {
                        String deviceName = json.getString("device_name");
                        String deviceOSVer = json.getString("device_os_ver");
                        String ip = json.getString("client_ip");
                        String key = toDay + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER + childID + ":" + ip + ":" + deviceName + ":" + deviceOSVer;

                        String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
                        value = value == null ? "0" : value;
                        if (Integer.parseInt(value) < ServiceConstant.ACTIVE_COUNT) {
                            JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, key);
                            imei = deviceName + ":" + deviceOSVer + ":" + ip + ":" + value;
                            startupDeviceFlag = true;
                        }
                    } else {
                        imei = idfa;
                        startupDeviceFlag = true;
                    }
                } else {
                    startupDeviceFlag = true;
                }
                if (startupDeviceFlag) {
                    // 启动设备数
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


                    // 启动设备数
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
            }
        });

        ios.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> map = new HashMap<>(16);

            @Override
            public Boolean call(JSONObject json) throws Exception {
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
                String time = DateUtil.getNowFutureWhileMinute(json.getLong("ts"));
                String toDay = DateUtil.secondToDateString(second , DateUtil.YYYY_MM_DD);


                SaveDataUtils.saveDeviceInstallData(toDay, appID, childID, channelID, appChannelID, packageID);
                SaveMinuteDataUtils.saveMinuteDeviceInstallData(time, appID ,childID , channelID , appChannelID , packageID);
            }
        });
    }
}
