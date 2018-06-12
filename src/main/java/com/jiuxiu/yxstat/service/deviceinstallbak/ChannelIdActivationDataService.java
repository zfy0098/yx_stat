package com.jiuxiu.yxstat.service.deviceinstallbak;

import com.jiuxiu.yxstat.dao.stat.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisChannelIDActivationKeyConstant;
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
 * Created by ZhouFy on 2018/6/5.
 *
 * @author ZhouFy
 */
public class ChannelIdActivationDataService implements Serializable {

    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();


    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();


    private static String type = "CHANNEL_ID";


    public static void ChannelActivationData(JavaRDD<JSONObject> javaRDD){

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        //  channel id 启动次数
        javaRDD.mapToPair(new PairFunction<JSONObject, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(JSONObject jsonObject) throws Exception {
                return new Tuple2<>(jsonObject.getInt("channel_id") , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                String key = toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_COUNT +  tuple2._1;
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                value = value == null ? "0":value ;
                int count = Integer.parseInt(value) + tuple2._2;
                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , key , String.valueOf(count) , 0);
                statChannelIdDeviceActiveDao.saveChannelIdStartUpCount(new Object[]{tuple2._1 , count  , count });
            }
        });


        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                String id = json.getString("imei");
                if(StringUtils.isEmpty(id)){
                    id = json.getString("idfa");
                }

                int channelID = json.getInt("channel_id");

                if(!StringUtils.isEmpty(id)){
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallForTypeIDByImei(id ,type , channelID);
                    if(getResponse == null || getResponse.getSource() == null){
                        // 没有激活
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json ,type , channelID);
                    }

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" + id);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_INFO + channelID + ":" +  id  , json.toString() , 0);
                    }
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                // 新增设备数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + jsonObject.getInt("channel_id"));
                value = value == null ? "0" : value ;
                statChannelIdDeviceActiveDao.saveChannelIdNewDeviceCount(new Object[]{jsonObject.getInt("channel_id") , value , value });

                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisChannelIDActivationKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + jsonObject.getInt("channel_id"));
                value = value == null ? "0" : value ;
                statChannelIdDeviceActiveDao.saveChannelIdDeviceStartUpCount(new Object[]{ jsonObject.getInt("channel_id")  , value , value });
            }
        });
    }

}
