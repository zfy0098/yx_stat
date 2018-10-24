package com.jiuxiu.yxstat.service.user;

import com.jiuxiu.yxstat.dao.stat.userstatistics.StatUserRegisterLoginAppChannelIDDao;
import com.jiuxiu.yxstat.dao.stat.userstatistics.StatUserRegisterLoginAppIDDao;
import com.jiuxiu.yxstat.dao.stat.userstatistics.StatUserRegisterLoginChannelIDDao;
import com.jiuxiu.yxstat.dao.stat.userstatistics.StatUserRegisterLoginChildIDDao;
import com.jiuxiu.yxstat.dao.stat.userstatistics.StatUserRegisterLoginPackageIDDao;
import com.jiuxiu.yxstat.enums.RegisterType;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.userstatistics.JedisUserStatisticsKeyConstant;

/**
 * Created with IDEA by ZhouFy on 2018/6/29.
 *
 * @author ZhouFy
 */
public class SaveDataUtils {

    private static StatUserRegisterLoginAppIDDao statUserRegisterLoginAppIDDao = StatUserRegisterLoginAppIDDao.getInstance();

    private static StatUserRegisterLoginChildIDDao statUserRegisterLoginChildIDDao = StatUserRegisterLoginChildIDDao.getInstance();

    private static StatUserRegisterLoginChannelIDDao statUserRegisterLoginChannelIDDao = StatUserRegisterLoginChannelIDDao.getInstance();

    private static StatUserRegisterLoginAppChannelIDDao statUserRegisterLoginAppChannelIDDao = StatUserRegisterLoginAppChannelIDDao.getInstance();

    private static StatUserRegisterLoginPackageIDDao statUserRegisterLoginPackageIDDao = StatUserRegisterLoginPackageIDDao.getInstance();


    public static void saveRegisterLoginCount(String date, int appID, int childID, int channelID, int appChannelID, int packageID) {

        long appIDGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.GUEST_REGISTER.getType());
        long appIDAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.ACCOUNT_REGISTER.getType());
        long appIDPhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.PHONE_REGISTER.getType());
        long appIDQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.QQ_REGISTER.getType());
        long appIDWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.WX_REGISTER.getType());
        long appIDOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_REGISTER_COUNT + appID + ":" + RegisterType.OTHER_REGISTER.getType());
        long appIDTotalRegisterCount = appIDGuestRegisterCount + appIDAccountRegisterCount + appIDPhoneRegisterCount + appIDQQRegisterCount + appIDWXRegisterCount + appIDOtherRegisterCount;
        long appIDLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_ID_LOGIN_COUNT + appID);


        if (appIDGuestRegisterCount != 0 || appIDAccountRegisterCount != 0 || appIDPhoneRegisterCount != 0 || appIDQQRegisterCount != 0
                || appIDWXRegisterCount != 0 || appIDLoginCount != 0) {
            statUserRegisterLoginAppIDDao.saveAppIDRegisterLoginCount(date ,appID, appIDLoginCount, appIDGuestRegisterCount, appIDAccountRegisterCount, appIDPhoneRegisterCount,
                    appIDQQRegisterCount, appIDWXRegisterCount, appIDOtherRegisterCount,appIDTotalRegisterCount);
        }

        long childIDGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + RegisterType.GUEST_REGISTER.getType());
        long childIDAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + RegisterType.ACCOUNT_REGISTER.getType());
        long childIDPhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + RegisterType.PHONE_REGISTER.getType());
        long childIDQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + RegisterType.QQ_REGISTER.getType());
        long childIDWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" + RegisterType.WX_REGISTER.getType());
        long childIDOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date +  JedisUserStatisticsKeyConstant.CHILD_ID_REGISTER_COUNT + childID + ":" + appID + ":" +RegisterType.OTHER_REGISTER.getType());
        long childIDTotalRegisterCount = childIDGuestRegisterCount + childIDAccountRegisterCount + childIDPhoneRegisterCount + childIDQQRegisterCount + childIDWXRegisterCount + childIDOtherRegisterCount;
        long childIDLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHILD_ID_LOGIN_COUNT + childID + ":" + appID);


        if (childIDGuestRegisterCount != 0 || childIDAccountRegisterCount != 0 || childIDPhoneRegisterCount != 0 || childIDQQRegisterCount != 0
                || childIDWXRegisterCount != 0 || childIDLoginCount != 0) {
            statUserRegisterLoginChildIDDao.saveChildIDRegisterLoginCount(date, appID, childID, childIDLoginCount, childIDGuestRegisterCount, childIDAccountRegisterCount, childIDPhoneRegisterCount, childIDQQRegisterCount, childIDWXRegisterCount, childIDOtherRegisterCount, childIDTotalRegisterCount);
        }

        long channelIDGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.GUEST_REGISTER.getType());
        long channelIDAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.ACCOUNT_REGISTER.getType());
        long channelIDPhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.PHONE_REGISTER.getType());
        long channelIDQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.QQ_REGISTER.getType());
        long channelIDWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.WX_REGISTER.getType());
        long channelIDOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_REGISTER_COUNT + channelID + ":" + childID + ":" + appID + ":" + RegisterType.OTHER_REGISTER.getType());
        long channelIDTotalRegisterCount = channelIDGuestRegisterCount + channelIDAccountRegisterCount + channelIDPhoneRegisterCount + channelIDQQRegisterCount + channelIDWXRegisterCount + channelIDOtherRegisterCount;
        long channelIDLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.CHANNEL_ID_LOGIN_COUNT + channelID + ":" + childID + ":" + appID);

        if (channelIDGuestRegisterCount != 0 || channelIDAccountRegisterCount != 0 || channelIDPhoneRegisterCount != 0 || channelIDQQRegisterCount != 0
                || channelIDWXRegisterCount != 0 || channelIDLoginCount != 0) {
            statUserRegisterLoginChannelIDDao.saveChannelIDRegisterLoginCount(date, appID, childID, channelID, channelIDLoginCount, channelIDGuestRegisterCount, channelIDAccountRegisterCount,
                    channelIDPhoneRegisterCount, channelIDQQRegisterCount, channelIDWXRegisterCount, channelIDOtherRegisterCount, channelIDTotalRegisterCount);
        }

        long appChannelIDGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.GUEST_REGISTER.getType());
        long appChannelIDAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.ACCOUNT_REGISTER.getType());
        long appChannelIDPhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.PHONE_REGISTER.getType());
        long appChannelIDQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.QQ_REGISTER.getType());
        long appChannelIDWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.WX_REGISTER.getType());
        long appChannelIDOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_REGISTER_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.OTHER_REGISTER.getType());
        long appChannelIDTotalRegisterCount = appChannelIDGuestRegisterCount + appChannelIDAccountRegisterCount + appChannelIDPhoneRegisterCount + appChannelIDQQRegisterCount + appChannelIDWXRegisterCount + appChannelIDOtherRegisterCount;
        long appChannelIDLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.APP_CHANNEL_ID_LOGIN_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);

        if (appChannelIDGuestRegisterCount != 0 || appChannelIDAccountRegisterCount != 0 || appChannelIDPhoneRegisterCount != 0 || appChannelIDQQRegisterCount != 0
                || appChannelIDWXRegisterCount != 0 || appChannelIDLoginCount != 0) {
            statUserRegisterLoginAppChannelIDDao.saveAppChannelIDRegisterLoginCount(date, appID, childID, channelID, appChannelID, appChannelIDLoginCount, appChannelIDGuestRegisterCount,
                    appChannelIDAccountRegisterCount, appChannelIDPhoneRegisterCount, appChannelIDQQRegisterCount, appChannelIDWXRegisterCount, appChannelIDOtherRegisterCount, appChannelIDTotalRegisterCount);
        }

        long packageIDGuestRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.GUEST_REGISTER.getType());
        long packageIDAccountRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.ACCOUNT_REGISTER.getType());
        long packageIDPhoneRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.PHONE_REGISTER.getType());
        long packageIDQQRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.QQ_REGISTER.getType());
        long packageIDWXRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.WX_REGISTER.getType());
        long packageIDOtherRegisterCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_REGISTER_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID + ":" + RegisterType.OTHER_REGISTER.getType());
        long packageIDTotalRegisterCount = packageIDGuestRegisterCount + packageIDAccountRegisterCount + packageIDPhoneRegisterCount + packageIDQQRegisterCount + packageIDWXRegisterCount + packageIDOtherRegisterCount;
        long packageIDLoginCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, date + JedisUserStatisticsKeyConstant.PACKAGE_ID_LOGIN_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);

        if (packageIDGuestRegisterCount != 0 || packageIDAccountRegisterCount != 0 || packageIDPhoneRegisterCount != 0 || packageIDQQRegisterCount != 0
                || packageIDWXRegisterCount != 0 || packageIDLoginCount != 0) {
            statUserRegisterLoginPackageIDDao.savePackageIDRegisterLoginCount(date, appID, childID, channelID, appChannelID, packageID, packageIDLoginCount, packageIDGuestRegisterCount, packageIDAccountRegisterCount,
                    packageIDPhoneRegisterCount, packageIDQQRegisterCount, packageIDWXRegisterCount, packageIDTotalRegisterCount, packageIDTotalRegisterCount);
        }
    }
}
