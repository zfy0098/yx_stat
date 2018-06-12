package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.StatActivationStatisticsDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisChildIDActivationKeyConstant;
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
import org.elasticsearch.action.get.GetResponse;
import scala.Tuple2;

import java.io.Serializable;

public class ActivationStatisticsService implements Serializable {


    private static StatActivationStatisticsDao statActivationStatisticsDao = StatActivationStatisticsDao.getInstance();

    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();

    private static String type = "CHILD_ID";

    public static void activationData(JavaRDD<JSONObject> javaRDD){

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        // 计算统计数
        startupCount(javaRDD);

        javaRDD.filter(new Function<JSONObject, Boolean>() {
            @Override
            public Boolean call(JSONObject jsonObject) throws Exception {
                return 1 == jsonObject.getInt("os");
            }
        }).foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {

            }
        });






        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {


                String id = json.getString("imei");
                if(StringUtils.isEmpty(id)){
                    id = json.getString("idfa");
                }

                int childID = json.getInt("child_id");

                if(StringUtils.isEmpty(id)){
                    //  没有imei 和idfa 唯一值
                    String key = JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT
                            + childID + ":" + json.getString("client_ip")  + ":" +  json.getString("device_name")  + ":"
                            + json.getString("device_os_ver");

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                    value = value == null ? "0" : value ;

                    if(Integer.parseInt(value) < ServiceConstant.ACTIVE_COUNT){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , key);
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json, type,childID);
                    }

                    String countKey = toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER +
                            childID + ":"+  json.getString("client_ip")  + ":" +  json.getString("device_name")  + ":"  + json.getString("device_os_ver");

                    String count = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , countKey);

                    count = count == null ? "0" : count;
                    if(Integer.parseInt(count) < ServiceConstant.ACTIVE_COUNT){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , countKey);
                    }
                }else{
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallForTypeIDByImei(id ,type , childID);
                    if(getResponse == null || getResponse.getSource() == null){
                        // 没有激活
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json ,type , childID);
                    }
                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" + id);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_INFO + childID + ":" +  id  , json.toString() , 0);
                    }
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {

                // 新增设备数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + jsonObject.getInt("child_id"));
                value = value == null ? "0" : value ;
                statChildDeviceActiveDao.saveChildIdNewDeviceCount(new Object[]{jsonObject.getInt("child_id") , value , value });

                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + jsonObject.getInt("child_id"));
                value = value == null ? "0" : value ;
                statChildDeviceActiveDao.saveChildIdDeviceStartUpCount(new Object[]{ jsonObject.getInt("child_id")  , value , value });
            }
        });
    }

    /**
     *  计算启动数
     * @param javaRDD
     */
    public static void startupCount(JavaRDD<JSONObject> javaRDD){

        javaRDD.mapToPair(new PairFunction<JSONObject, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(JSONObject json) throws Exception {
                StringBuffer key = new StringBuffer(json.getInt("package_id"));
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
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple2) throws Exception {
                String key = tuple2._1;
                String[] keys = key.split("#");
                if(keys.length == 5){
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
