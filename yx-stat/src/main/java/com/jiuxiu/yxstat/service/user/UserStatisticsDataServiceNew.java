package com.jiuxiu.yxstat.service.user;

import com.jiuxiu.yxstat.redis.CacheKey;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.userstatistics.JedisUserStatisticsKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by ZhouFy on 2018/6/27.
 *
 * @author ZhouFy
 */
public class UserStatisticsDataServiceNew implements Runnable, Serializable {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private JavaRDD<JSONObject> javaRDD;

    UserStatisticsDataServiceNew(JavaRDD<JSONObject> javaRDD) {
        this.javaRDD = javaRDD;
    }

    @Override
    public void run() {
        javaRDD = javaRDD.filter(json -> {

            int appID = json.getInt("appid");
            int childID = json.getInt("child_id");
            int channelID = json.getInt("channel_id");
            int appChannelID = json.getInt("app_channel_id");
            int packageID = json.getInt("package_id");
            String uid = json.getString("uid");
            String imei = json.getString("imei");

            int os = json.getInt("os");
            if(os == ServiceConstant.IOS_OS){
                String value = JedisUtils.get(JedisPoolConfigInfo.adClickPoolKey,  CacheKey.IOS_CLICK_INFO + imei + ":" + appID);
                if(StringUtils.isEmpty(value)){
                    JSONObject redisInfo = JSONObject.fromObject(value);

                    appID = redisInfo.getInt("appid");
                    childID = redisInfo.getInt("child_id");
                    channelID = redisInfo.getInt("channel_id");
                    appChannelID = redisInfo.getInt("app_channel_id");
                    packageID = redisInfo.getInt("package_id");

                    json.put("appid", appID);
                    json.put("child_id", childID);
                    json.put("channel_id", channelID);
                    json.put("app_channel_id", appChannelID);
                    json.put("package_id", packageID);
                }
            }

            long ts = json.getLong("ts");
            String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
            String time = DateUtil.getNowFutureWhileMinute(ts);

            String registerTypeKey = "register_type";
            // 用户注册
            logger.info("注册数据：" + json.toString());
            if (json.has(registerTypeKey)) {
                int type = json.getInt(registerTypeKey);
                /*
                 * 保存注册数据
                 */
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + type,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);

                /*
                 *  保存分钟注册数据
                 */
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + type,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + type,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);

            } else {
                /*
                 *   保存登录数据
                 */
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_LOGIN_COUNT + appID,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_LOGIN_COUNT + childID + ":" + appID,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_LOGIN_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_LOGIN_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_LOGIN_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);

                /*
                 *  保存分钟登录数据
                 */
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.APP_ID_LOGIN_COUNT + appID,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid + ":" + childID);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.CHILD_ID_LOGIN_COUNT + childID + ":" + appID,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.CHANNEL_ID_LOGIN_COUNT + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_LOGIN_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
                JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PACKAGE_ID_LOGIN_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID,
                        ServiceConstant.USER_INFO_MINUTE_REDIS_EXPIRE_TIME, uid);
            }
            return true;
        });

        javaRDD.filter(new Function<JSONObject, Boolean>() {
            Map<String, JSONObject> map = new HashMap<>(16);
            @Override
            public Boolean call(JSONObject json) throws Exception {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int channelID = json.getInt("channel_id");
                int appChannelID = json.getInt("app_channel_id");
                int packageID = json.getInt("package_id");
                long ts = json.getLong("ts");
                String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                StringBuilder key = new StringBuilder();
                key.append(appID);
                key.append(childID);
                key.append(channelID);
                key.append(appChannelID);
                key.append(packageID);
                key.append(date);
                key.append(time);

                if (map.get(key.toString()) == null) {
                    map.put(key.toString(), json);
                    return true;
                }
                return false;
            }
        }).foreach(json -> {
                int appID = json.getInt("appid");
                int childID = json.getInt("child_id");
                int channelID = json.getInt("channel_id");
                int appChannelID = json.getInt("app_channel_id");
                int packageID = json.getInt("package_id");

                long ts = json.getLong("ts");

                String date = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                // 保存天数据
                SaveDataUtils.saveRegisterLoginCount(date, appID, childID, channelID, appChannelID, packageID);

                // 保存分钟数据
                SaveUserMinuteDataUtils.saveMinuteRegisterLoginCount(time, appID, childID, channelID, appChannelID, packageID);
        });
    }
}
