package com.jiuxiu.yxstat.service.user;

import com.jiuxiu.yxstat.dao.stat.userstatistics.UserRegisterLoginDao;
import com.jiuxiu.yxstat.enums.RegisterType;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.userstatistics.JedisUserStatisticsKeyConstant;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.utils.DateUtil;
import net.sf.json.JSONObject;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;

import java.io.Serializable;

/**
 * Created with IDEA by ZhouFy on 2018/6/27.
 *
 * @author ZhouFy
 */
public class PlatformUserStatisticsDataService implements Runnable, Serializable {

    private JavaRDD<JSONObject> javaRDD;

    private UserRegisterLoginDao userRegisterLoginDao = UserRegisterLoginDao.getInstance();

    PlatformUserStatisticsDataService(JavaRDD<JSONObject> javaRDD) {
        this.javaRDD = javaRDD;
    }

    @Override
    public void run() {

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) {
                String uid = json.getString("uid");
                long ts = json.getLong("ts");
                String date = DateUtil.secondToDateString(json.getLong("ts"), DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                String registerTypeKey = "register_type";
                if (json.has(registerTypeKey)) {
                    int type = json.getInt(registerTypeKey);
                    // 保存注册数据
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + type,
                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);

                    // 保存分钟注册数据
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + type,
                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);

                } else {
                    // 保存登录数据
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PLATFORM_LOGIN_COUNT,
                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);

                    // 保存分钟登录数据
                    JedisUtils.setSetAdd(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_LOGIN_COUNT,
                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME, uid);
                }
            }
        });

        javaRDD.foreach(new VoidFunction<JSONObject>() {
            @Override
            public void call(JSONObject json) throws Exception {

                long ts = json.getLong("ts");
                String toDay = DateUtil.secondToDateString(ts, DateUtil.YYYY_MM_DD);
                String time = DateUtil.getNowFutureWhileMinute(ts);

                // 用户注册数量
                long loginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_LOGIN_COUNT);
                long guestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.GUEST_REGISTER.getType());
                long accountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.ACCOUNT_REGISTER.getType());
                long phoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.PHONE_REGISTER.getType());
                long qqRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.QQ_REGISTER.getType());
                long wxRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.WX_REGISTER.getType());
                long otherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.OTHER_REGISTER.getType());


                int zero = 0;
                // 保存数据库
                if (zero != loginCount || zero != guestRegisterCount || zero != accountRegisterCount || zero != phoneRegisterCount ||
                        zero != qqRegisterCount || zero != wxRegisterCount) {
                    userRegisterLoginDao.savePlatformUserRegisterLoginCount(toDay, loginCount, guestRegisterCount, accountRegisterCount, phoneRegisterCount, qqRegisterCount, wxRegisterCount , otherRegisterCount);
                }

                // 分钟数据
                long minuteLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_LOGIN_COUNT);
                long minuteGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.GUEST_REGISTER.getType());
                long minuteAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.ACCOUNT_REGISTER.getType());
                long minutePhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.PHONE_REGISTER.getType());
                long minuteQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.QQ_REGISTER.getType());
                long minuteWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.WX_REGISTER.getType());
                long minuteOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisUserStatisticsKeyConstant.PLATFORM_REGISTER_COUNT + RegisterType.OTHER_REGISTER.getType());



                userRegisterLoginDao.savePlatformMinuteRegisterLoginCount(time, minuteLoginCount, minuteGuestRegisterCount, minuteAccountRegisterCount, minutePhoneRegisterCount,
                        minuteQQRegisterCount, minuteWXRegisterCount , minuteOtherRegisterCount);
            }
        });
    }
}
