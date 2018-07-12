package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatDeviceActiveDao;
import com.jiuxiu.yxstat.es.deviceinstall.PlatformDeviceInstallESStorage;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisPlatformDeviceActivationKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.get.GetResponse;
import redis.clients.jedis.Jedis;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/14.
 *
 * @author ZhouFy
 */
public class PlatformAndroidActivationDataService implements Runnable,  Serializable {

    private PlatformDeviceInstallESStorage deviceInstallInfo = PlatformDeviceInstallESStorage.getInstance();
    private StatDeviceActiveDao statDeviceActiveDao = StatDeviceActiveDao.getInstance();
    private JavaRDD<JSONObject> android;

    PlatformAndroidActivationDataService(JavaRDD<JSONObject> android) {
        this.android = android;
    }

    @Override
    public void run() {
        // android 启动次数
        android.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                long ts = jsonObject.getLong("ts");
                String toDay = DateUtil.secondToDateString(ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);
                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);
                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);
            }
        });
        android.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> androidDevice = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject json) throws Exception {
                String key = json.getString("imei") + DateUtil.secondToDateString(json.getLong("ts") , DateUtil.YYYY_MM_DD);
                if (androidDevice.get(key) == null) {
                    androidDevice.put(key , json);
                    return true;
                }
                return false;
            }
        }).foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                String imei = json.getString("imei");
                long ts = json.getLong("ts");
                String toDay = DateUtil.secondToDateString(ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);
                GetResponse response = deviceInstallInfo.getDeviceInstallByID(imei);
                if (response == null || response.getSource() == null) {
                    // android 没有激活设备 将设备信息保存到es中
                    deviceInstallInfo.saveDeviceInstall(json);
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , imei);

                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT,
                            ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME , imei);
                }
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);

                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT,
                        ServiceConstant.DEVICE_ACTIVATION_EXPIRE_TIME, imei);
            }
        });

        android.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {

                long ts = jsonObject.getLong("ts");

                String toDay = DateUtil.secondToDateString(ts , DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                // android 新设备数
                long androidNewDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT);
                //  android 设备启动数
                long androidDeviceStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT);
                //  android 启动数
                String androidStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);

                int zero = 0;
                long startupCount  = androidStartUpCount == null ? zero : Long.parseLong(androidStartUpCount);

                if (androidNewDeviceCount != zero || androidDeviceStartUpCount != zero || startupCount !=  zero) {
                    //  保存数据库操作
                    statDeviceActiveDao.saveDeviceActiveCount(toDay , androidNewDeviceCount, androidDeviceStartUpCount, startupCount , ServiceConstant.ANDROID_OS);
                }

                long minuteNewDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_NEW_DEVICE_COUNT);
                long minuteDeviceStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_DEVICE_COUNT);
                String minuteStartUpCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, time + JedisPlatformDeviceActivationKeyConstant.ANDROID_STARTUP_COUNT);

                long minuteStart = minuteStartUpCount == null ? zero : Long.parseLong(minuteStartUpCount);

                if(minuteNewDeviceCount !=0 || minuteDeviceStartUpCount != 0 || minuteStart !=0 ){
                    statDeviceActiveDao.saveMinuteDeviceActiveCount(time, minuteNewDeviceCount,minuteDeviceStartUpCount, minuteStart, ServiceConstant.ANDROID_OS);
                }
            }
        });
    }
}
