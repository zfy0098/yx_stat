package com.jiuxiu.yxstat.service.pay;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.payorder.JedisPayOrderKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/7/10.
 *
 * @author ZhouFy
 */
public class PlatformPayOrderDataService implements Serializable {

    private static PlatformPayOrderDataService platformPayOrderDataService = new PlatformPayOrderDataService();

    private PlatformPayOrderDataService() {
    }

    public static PlatformPayOrderDataService getInstance() {
        return platformPayOrderDataService;
    }

    Object readResolve() {
        return platformPayOrderDataService;
    }

    public void platformPayOrderData(JavaRDD<JSONObject> javaRDD) {

        javaRDD.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> map = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject jsonObject) throws Exception {
                String uid = jsonObject.getString("uid");
                if (map.get(uid) == null) {
                    map.put(uid, jsonObject);
                    return true;
                }
                return false;
            }
        }).foreach(json -> {
            String uid = json.getString("uid");
            long ts = json.getLong("ts");
            String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
            JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisPayOrderKeyConstant.PLATFORM_PAY_ORDER, ServiceConstant.PAY_ORDER_INFO_REDIS_EXPIRE_TIME, uid);
        });

        javaRDD.mapToPair(json -> {
            long ts = json.getLong("ts");
            String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);

            Map<String, Integer> map = new HashMap<>(4);
            map.put("payOrderCount", 1);
            map.put("payTotalAmount", json.getInt("money"));
            return new Tuple2<>(date, map);
        }).reduceByKey((stringIntegerMap, stringIntegerMap2) -> {
            Map<String, Integer> map = new HashMap<>(4);
            map.put("payOrderCount", stringIntegerMap.get("payOrderCount") + stringIntegerMap2.get("payOrderCount"));
            map.put("payTotalAmount", stringIntegerMap.get("payTotalAmount") + stringIntegerMap2.get("payTotalAmount"));
            return map;
        }).foreach(tuple2 -> {
            String date = tuple2._1;
            int payOrderCount = tuple2._2.get("payOrderCount");
            int payTotalAmount = tuple2._2.get("payTotalAmount");
            SavePayOrderDataUtils.savePlatformPayOrderData(date, payOrderCount, payTotalAmount);
        });
    }
}
