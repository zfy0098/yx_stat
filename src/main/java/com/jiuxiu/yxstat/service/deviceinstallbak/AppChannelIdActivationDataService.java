package com.jiuxiu.yxstat.service.deviceinstallbak;

import com.jiuxiu.yxstat.dao.stat.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisAppChannelIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.elasticsearch.action.get.GetResponse;
import scala.Tuple2;

import java.io.Serializable;

/**
 * @author admin
 */
public class AppChannelIdActivationDataService  implements Serializable{

    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static String type = "APP_CHANNEL_ID";

    public static void appChannelIDActivationData(JavaRDD<JSONObject> javaRDD){

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        //   app channel id  启动次数
        javaRDD.mapToPair(new PairFunction<JSONObject, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(JSONObject jsonObject) throws Exception {
                return new Tuple2<>(jsonObject.getInt("app_channel_id") , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                System.out.println("app channel id :" + tuple2._1 + " ,  count :" + tuple2._2);
                String key = toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_COUNT +  tuple2._1;
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                value = value == null ? "0":value ;
                int count = Integer.parseInt(value) + tuple2._2;
                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , key , String.valueOf(count) , 0);
                statAppChannelIdDeviceActiveDao.saveAppChannelIdStartUpCount(new Object[]{tuple2._1 , count  , count });
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                String id = json.getString("imei");
                if(StringUtils.isEmpty(id)){
                    id = json.getString("idfa");
                }

                int appChannelID = json.getInt("app_channel_id");

                if(!StringUtils.isEmpty(id)){
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallForTypeIDByImei(id ,type , appChannelID);
                    if(getResponse == null || getResponse.getSource() == null){
                        // 没有激活
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json ,type , appChannelID);
                    }

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" + id);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_INFO + appChannelID + ":" +  id  , json.toString() , 0);
                    }
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                // 新增设备数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + jsonObject.getInt("app_channel_id"));
                value = value == null ? "0" : value ;
                statAppChannelIdDeviceActiveDao.saveAppChannelIdNewDeviceCount(new Object[]{jsonObject.getInt("app_channel_id") , value , value });

                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisAppChannelIDActivationKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + jsonObject.getInt("app_channel_id"));
                value = value == null ? "0" : value ;
                statAppChannelIdDeviceActiveDao.saveAppChannelIdDeviceStartUpCount(new Object[]{ jsonObject.getInt("app_channel_id")  , value , value });
            }
        });
    }


}
