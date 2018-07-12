package com.jiuxiu.yxstat.service.deviceinstall;


import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatDeviceActiveDao;
import com.jiuxiu.yxstat.es.deviceinstall.PlatformDeviceInstallESStorage;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisPlatformDeviceActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/19.
 *
 * @author ZhouFy
 */
public class PlatformIOSActivationDataService implements Runnable , Serializable{

    private PlatformDeviceInstallESStorage deviceInstallInfo = PlatformDeviceInstallESStorage.getInstance();
    private StatDeviceActiveDao statDeviceActiveDao = StatDeviceActiveDao.getInstance();

    private JavaRDD<JSONObject> ios;

    public PlatformIOSActivationDataService(JavaRDD<JSONObject> ios){
        this.ios = ios;
    }

    @Override
    public void run(){
        // 保存ios 数据启动数
        ios.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                long ts = json.getLong("ts");
                String toDay = DateUtil.secondToDateString(ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);
                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_COUNT);
                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_COUNT);
            }
        });
        ios.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> iosDevice = new HashMap<>(16);

            @Override
            public Boolean call(JSONObject jsonObject) throws Exception {
                String id = jsonObject.getString("imei");
                if (StringUtils.isEmpty(id)) {
                    id = jsonObject.getString("idfa");
                }
                if (StringUtils.isEmpty(id)) {
                    String deviceName = jsonObject.getString("device_name");
                    String deviceOSVer = jsonObject.getString("device_os_ver");
                    String ip = jsonObject.getString("client_ip");
                    id = deviceName + ":" + deviceOSVer + ":" + ip;
                }
                String key = id + DateUtil.secondToDateString(jsonObject.getLong("ts") , DateUtil.YYYY_MM_DD);

                if (iosDevice.get(key) == null) {
                    iosDevice.put(key, jsonObject);
                    return true;
                }
                return false;
            }
        }).foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                String id = jsonObject.getString("imei");
                if (StringUtils.isEmpty(id)) {
                    id = jsonObject.getString("idfa");
                }

                long ts = jsonObject.getLong("ts");
                String toDay = DateUtil.secondToDateString( ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                // 没有获取到ios的唯一值
                if (StringUtils.isEmpty(id)) {
                    String deviceName = jsonObject.getString("device_name");
                    String deviceOSVer = jsonObject.getString("device_os_ver");
                    String ip = jsonObject.getString("client_ip");

                    SearchResponse searchResponse = deviceInstallInfo.iosSpecialSearch(ip , deviceName , deviceOSVer);
                    int length = searchResponse.getHits().getHits().length;
                    if(length < ServiceConstant.ACTIVE_COUNT){
                        deviceInstallInfo.saveDeviceInstall(jsonObject);
                        JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT ,
                                ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , ip + ":" + deviceName + ":" + deviceOSVer + ":" + length);
                        JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey , time + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT ,
                                ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , ip + ":" + deviceName + ":" + deviceOSVer + ":" + length);
                    }
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT ,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , ip + ":" + deviceName + ":" + deviceOSVer + ":" + length );

                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  , time + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT ,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , ip + ":" + deviceName + ":" + deviceOSVer + ":" + length );
                } else {
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallByID(id);
                    if (getResponse == null || getResponse.getSource() == null) {
                        deviceInstallInfo.saveDeviceInstall(jsonObject);
                        JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT ,
                                ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , id);

                        JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  ,  time + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT ,
                                ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , id);
                    }
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT ,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , id );

                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey  , time + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT ,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , id );
                }
            }
        });


        ios.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {

                long ts = jsonObject.getLong("ts");
                String toDay = DateUtil.secondToDateString(ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                // ios新增设备数
                long iosNewDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT);
                // ios启动设备数
                long iosDeviceStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT);
                // ios启动次数
                String iosStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_COUNT);

                int zero = 0;

                long startupCount = iosStartUpCount == null ? zero : Long.parseLong(iosStartUpCount);

                if (iosNewDeviceCount != zero || iosDeviceStartUpCount != zero || startupCount != zero) {
                    //  保存数据库操作
                    statDeviceActiveDao.saveDeviceActiveCount(toDay , iosNewDeviceCount, iosDeviceStartUpCount, startupCount, ServiceConstant.IOS_OS);
                }

                // ios新增设备数
                long minuteNewDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.IOS_NEW_DEVICE_COUNT);
                // ios启动设备数
                long minuteDeviceStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_DEVICE_COUNT);
                // ios启动次数
                String minuteStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.IOS_STARTUP_COUNT);

                long minuteStartup = minuteStartUpCount == null ? zero : Long.parseLong(minuteStartUpCount);

                if (minuteNewDeviceCount != zero || minuteDeviceStartUpCount != zero || minuteStartup != zero) {
                    //  保存数据库操作
                    statDeviceActiveDao.saveMinuteDeviceActiveCount(time , minuteNewDeviceCount, minuteDeviceStartUpCount, minuteStartup, ServiceConstant.IOS_OS);
                }
            }
        });
    }
}
