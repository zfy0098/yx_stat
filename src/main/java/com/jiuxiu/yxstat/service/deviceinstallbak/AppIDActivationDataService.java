package com.jiuxiu.yxstat.service.deviceinstallbak;

import com.jiuxiu.yxstat.dao.stat.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisAppIDActivationKeyConstant;
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

public class AppIDActivationDataService implements Serializable{

    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static String type = "APP_ID";


    public static void appidActivationData(JavaRDD<JSONObject> javaRDD){


        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        // app id 启动次数
        javaRDD.mapToPair(new PairFunction<JSONObject, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(JSONObject jsonObject) throws Exception {
                return new Tuple2<>(jsonObject.getInt("appid") , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                System.out.println("appid ：" + tuple2._1 + " ,  count:" + tuple2._2);
                String key = toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_COUNT +  tuple2._1;
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                value = value == null ? "0":value ;
                int count = Integer.parseInt(value) + tuple2._2;
                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , key , String.valueOf(count) , 0);
                statAppIdDeviceActiveDao.saveAppIdStartUpCount(new Object[]{tuple2._1 , count  , count });
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                int appid = json.getInt("appid");
                String id = json.getString("imei");
                if(StringUtils.isEmpty(id)){
                    id = json.getString("idfa");
                }
                if(!StringUtils.isEmpty(id)){
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallForTypeIDByImei(id ,type , appid);
                    if(getResponse == null || getResponse.getSource() == null){
                        // 没有激活
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + appid);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json ,type , appid);
                    }

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appid + ":" + id);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appid);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_INFO + appid + ":" +  id  , json.toString() , 0);
                    }
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                // 新增设备数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_NEW_DEVICE_COUNT + jsonObject.getInt("appid"));
                value = value == null ? "0" : value ;
                statAppIdDeviceActiveDao.saveAppIdNewDeviceCount(new Object[]{jsonObject.getInt("appid") , value , value });

                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  ,  toDay + JedisAppIDActivationKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + jsonObject.getInt("appid"));
                value = value == null ? "0" : value ;
                statAppIdDeviceActiveDao.saveAppIdDeviceStartUpCount(new Object[]{ jsonObject.getInt("appid")  , value , value });
            }
        });

    }
}
