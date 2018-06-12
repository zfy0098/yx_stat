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
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.search.SearchResponse;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author Zhoufy
 */
public class ActivationStatisticsService implements Serializable {

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();

    private static DeviceActivationStatisticsESStorage deviceActivationStatisticsESStorage = DeviceActivationStatisticsESStorage.getInstance();

    private static StatActivationStatisticsDao statActivationStatisticsDao = StatActivationStatisticsDao.getInstance();

    public static void activationData(JavaRDD<JSONObject> javaRDD){
        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);
        // 计算统计数
        startupCount(javaRDD);

        //  android 数据
        JavaRDD<JSONObject> android = javaRDD.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject jsonObject) {
                return 1 == jsonObject.getInt("os");
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
                String imei = json.getString("imei");
                SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei , appID , childID);
                if(searchResponse == null || searchResponse.getHits() == null || searchResponse.getHits().getHits() == null || searchResponse.getHits().getHits().length == 0){
                    // 没有激活
                    deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json , appID);
                    //  保存 child id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                    //  保存 app id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
                    //  保存 app channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                    //  保存 channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                    //  保存 package id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                }
                // child id 启动数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" + imei);
                if(value == null){
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" +  imei  , json.toString() , 0);
                }
                //  app channel id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" + imei);
                if(value == null){
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" +  imei  , json.toString() , 0);
                }
                //  app id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appID + ":" + imei);
                if(value == null){
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appID + ":" +  imei  , json.toString() , 0);
                }
                //  channel id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" + imei);
                if(value == null){
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" +  imei  , json.toString() , 0);
                }
                //  package id 启动数
                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" + imei);
                if(value == null){
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" +  imei  , json.toString() , 0);
                }
            }
        });
        android.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json)  {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                /*
                 *   保存 新增设备数
                 */
                // 保存 child id 新增设备数 (激活数量)
                String childIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                if(childIDNewDeviceCount != null){
                    statChildDeviceActiveDao.saveChildIdNewDeviceCount(new Object[]{childID , childIDNewDeviceCount , childIDNewDeviceCount});
                }
                // 保存 app Channel id 新增设备数
                String appChannelIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                if(appChannelIDNewDeviceCount != null){
                    statAppChannelIdDeviceActiveDao.saveAppChannelIdNewDeviceCount(new Object[]{appChannelID , appChannelIDNewDeviceCount , appChannelIDNewDeviceCount});
                }
                // 保存 app id 新增设备数
                String appIDNewDeviceCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
                if(appIDNewDeviceCount != null){
                    statAppIdDeviceActiveDao.saveAppIdNewDeviceCount(new Object[]{appID , appIDNewDeviceCount , appIDNewDeviceCount});
                }
                // 保存 channel id 新增设备数
                String channelIDNewDevice = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                if(channelIDNewDevice != null){
                    statChannelIdDeviceActiveDao.saveChannelIdNewDeviceCount(new Object[]{channelID , channelIDNewDevice , channelIDNewDevice});
                }
                // 保存 package id 新增设备数
                String packageIDNewDevice = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                if(packageIDNewDevice != null){
                    statPackageIdDeviceActiveDao.savePackageIdNewDeviceCount(new Object[]{packageID , packageIDNewDevice , packageIDNewDevice});
                }

                /*
                 *  保存启动设备数
                 */
                // package id 启动设备数
                String packageIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                if(packageIDStartupCount != null){
                    statPackageIdDeviceActiveDao.savePackageIdDeviceStartUpCount(new Object[]{ packageID  , packageIDStartupCount , packageIDStartupCount });
                }
                // app channel id 启动设备数
                String appChannelIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID );
                if(appChannelIDStartupCount != null){
                    statAppChannelIdDeviceActiveDao.saveAppChannelIdDeviceStartUpCount(new Object[]{ appChannelID  , appChannelIDStartupCount , appChannelIDStartupCount });
                }
                // appid 启动设备数
                String appidStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
                if(appidStartupCount != null){
                    statAppIdDeviceActiveDao.saveAppIdDeviceStartUpCount(new Object[]{ appID  , appidStartupCount , appidStartupCount });
                }
                //  channel  id  启动设备数
                String channelIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                if(channelIDStartupCount != null){
                    statChannelIdDeviceActiveDao.saveChannelIdDeviceStartUpCount(new Object[]{channelID  , channelIDStartupCount , channelIDStartupCount });
                }
                // child id 启动设备数
                String childIDStartupCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                if(childIDStartupCount != null){
                    statChildDeviceActiveDao.saveChildIdDeviceStartUpCount(new Object[]{ childID , childIDStartupCount , childIDStartupCount });
                }
            }
        });

        // ios 数据
        JavaRDD<JSONObject> ios = javaRDD.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject jsonObject) {
                return 2 == jsonObject.getInt("os");
            }
        });

        ios.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int appChannelID = json.getInt("app_channel_id");
                int channelID = json.getInt("channel_id");
                int packageID = json.getInt("package_id");
                String imei = json.getString("imei");
                String idfa = json.getString("idfa");
                boolean flag = false;

                if(StringUtils.isEmpty(idfa)){
                    String deviceName = json.getString("device_name");
                    String deviceOSVer = json.getString("device_os_ver");
                    String ip = json.getString("client_ip");

                    SearchResponse searchResponse = deviceActivationStatisticsESStorage.iosSpecialSearchForAppID(ip , deviceName , deviceOSVer , appID , childID);
                    if(searchResponse == null || searchResponse.getHits()== null || searchResponse.getHits().getHits() == null ||
                            searchResponse.getHits().getHits().length < ServiceConstant.ACTIVE_COUNT){
                        flag = true;
                    }
                }else{
                    SearchResponse searchResponse = deviceActivationStatisticsESStorage.getAppDeviceActivationForImei(imei , appID , childID);
                    if(searchResponse == null || searchResponse.getHits()== null || searchResponse.getHits().getHits() == null ||
                            searchResponse.getHits().getHits().length == 0){
                        flag = true;
                    }
                }
                if(flag){
                    // 没有激活
                    deviceActivationStatisticsESStorage.saveDeviceInstallForAppID(json , appID);
                    //  保存 child id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                    //  保存 app id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
                    //  保存 app channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                    //  保存 channel id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                    //  保存 package id 激活数
                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                }

                //  启动设备数
                if(StringUtils.isEmpty(idfa)){

                    String deviceName = json.getString("device_name");
                    String deviceOSVer = json.getString("device_os_ver");
                    String ip = json.getString("client_ip");

                    String key = toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER
                            + childID + ":" + ip + ":"+ deviceName + ":" + deviceOSVer;

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, key);
                    value = value == null ? "0" : value;
                    if (Integer.parseInt(value) < ServiceConstant.ACTIVE_COUNT) {
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, key);
                        // child id 启动设备数加一
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.IOS_CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                        // app channel id 启动设备数加一
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.IOS_APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                        // app id 启动设备数加一
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.IOS_APP_ID_STARTUP_DEVICE_COUNT + appID);
                        // channel id 启动设备数加一
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.IOS_CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                        // package id 启动设备数加一
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.IOS_PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                    }
                }else{
                    // child id 启动设备数
                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.IOS_CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" + idfa);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.IOS_CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.IOS_CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" +  idfa  , json.toString() , 0);
                    }
                    //  app channel id 启动设备数
                    value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,toDay + JedisAppChannelIDActivationKeyConstant.IOS_APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" + idfa);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.IOS_APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.IOS_APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" +  idfa  , json.toString() , 0);
                    }
                    //  app id 启动设备数
                    value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.IOS_APP_ID_STARTUP_DEVICE_INFO + appID + ":" + idfa);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.IOS_APP_ID_STARTUP_DEVICE_COUNT + appID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.IOS_APP_ID_STARTUP_DEVICE_INFO + appID + ":" +  idfa  , json.toString() , 0);
                    }
                    //  channel id 启动设备数
                    value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.IOS_CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" + idfa);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.IOS_CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.IOS_CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" +  idfa  , json.toString() , 0);
                    }
                    //  package id 启动设备数
                    value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.IOS_PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" + idfa);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.IOS_PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.IOS_PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" +  idfa  , json.toString() , 0);
                    }
                }
            }
        });
    }

    /**
     *  计算启动数
     * @param javaRDD 数据
     */
    private static void startupCount(JavaRDD<JSONObject> javaRDD){

        javaRDD.mapToPair(new PairFunction<JSONObject, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(JSONObject json)  {
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
                return new Tuple2<>(key.toString() , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2)  {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2)  {
                String[] keys = tuple2._1.split("#");
                int keyLength = 5;
                if(keys.length == keyLength){
                    String packageID = keys[0];
                    String childID = keys[1];
                    String appChannelID = keys[2];
                    String channelID = keys[3];
                    String appid = keys[4];
                    statActivationStatisticsDao.savePackageIdStartUpCount(new Object[]{packageID , tuple2._2 , tuple2._2});
                    statActivationStatisticsDao.saveChildIDStartUpCount(new Object[]{childID , tuple2._2 , tuple2._2});
                    statActivationStatisticsDao.saveAppChannelIdStartUpCount(new Object[]{appChannelID , tuple2._2 , tuple2._2});
                    statActivationStatisticsDao.saveChannelIdStartUpCount(new Object[]{channelID , tuple2._2 , tuple2._2});
                    statActivationStatisticsDao.saveAppIdStartUpCount(new Object[]{appid , tuple2._2 , tuple2._2});
                }
            }
        });
    }
}
