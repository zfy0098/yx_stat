package com.jiuxiu.yxstat.service.deviceinstallbak;

import com.jiuxiu.yxstat.dao.stat.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisChildIDActivationKeyConstant;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.service.ServiceConstant;
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

public class ChildIdActivationDataService implements Serializable {

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();


    private static String type = "CHILD_ID";





    /**
     *  计算马甲包激活信息
     */
    public static void childIdActivationData(JavaRDD<JSONObject> javaRDD){

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        javaRDD.mapToPair(new PairFunction<JSONObject, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(JSONObject json) throws Exception {
                return new Tuple2<>(json.getInt("child_id") , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                //  child_id 启动次数
                System.out.println("child_id :" + tuple2._1  + " ,,  启动次数:" + tuple2._2);
                String key = toDay + JedisChildIDActivationKeyConstant.CHILD_ID_STARTUP_COUNT +  tuple2._1;
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                value = value == null ? "0":value ;
                int count = Integer.parseInt(value) + tuple2._2;
                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , key , String.valueOf(count) , 0);
                statChildDeviceActiveDao.saveChildIdStartUpCount(new Object[]{tuple2._1 , count  , count });
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

                System.out.println("执行操作数据库方法");

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
}
