package com.jiuxiu.yxstat.service.deviceinstallbak;

import com.jiuxiu.yxstat.dao.stat.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.es.DeviceInstallInfo;
import com.jiuxiu.yxstat.redis.JedisPackageIDActivationKeyConstant;
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
public class PackageActivationDataService implements Serializable{


    private static DeviceInstallInfo deviceInstallInfo = DeviceInstallInfo.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();

    private static String type = "PACKAGE_ID";

    public static void pacakgeActivationData(JavaRDD<JSONObject> javaRDD){

        String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

        //  channel id 启动次数
        javaRDD.mapToPair(new PairFunction<JSONObject, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(JSONObject jsonObject) throws Exception {
                return new Tuple2<>(jsonObject.getInt("package_id") , 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> tuple2) throws Exception {
                String key = toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_COUNT +  tuple2._1;
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , key);
                value = value == null ? "0":value ;
                int count = Integer.parseInt(value) + tuple2._2;
                JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , key , String.valueOf(count) , 0);
                statPackageIdDeviceActiveDao.savePackageIdStartUpCount(new Object[]{tuple2._1 , count  , count });
            }
        });


        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {
                String id = json.getString("imei");
                if(StringUtils.isEmpty(id)){
                    id = json.getString("idfa");
                }

                int packageID = json.getInt("package_id");

                if(!StringUtils.isEmpty(id)){
                    GetResponse getResponse = deviceInstallInfo.getDeviceInstallForTypeIDByImei(id ,type , packageID);
                    if(getResponse == null || getResponse.getSource() == null){
                        // 没有激活
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID);
                        deviceInstallInfo.saveDeviceInstallForTypeID(json ,type , packageID);
                    }

                    String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" + id);
                    if(value == null){
                        JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID);
                        JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_INFO + packageID + ":" +  id  , json.toString() , 0);
                    }
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject jsonObject) throws Exception {
                // 新增设备数
                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + jsonObject.getInt("package_id"));
                value = value == null ? "0" : value ;
                statPackageIdDeviceActiveDao.savePackageIdNewDeviceCount(new Object[]{jsonObject.getInt("package_id") , value , value });

                value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey  , toDay + JedisPackageIDActivationKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + jsonObject.getInt("package_id"));
                value = value == null ? "0" : value ;
                statPackageIdDeviceActiveDao.savePackageIdDeviceStartUpCount(new Object[]{ jsonObject.getInt("package_id")  , value , value });
            }
        });
    }
}
