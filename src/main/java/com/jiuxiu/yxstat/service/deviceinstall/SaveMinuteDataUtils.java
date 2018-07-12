package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceminute.StatAppChannelIdMinuteDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceminute.StatAppIdMinuteDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceminute.StatChannelIdMinuteDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceminute.StatChildMinuteDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceminute.StatPackageIdMinuteDeviceActiveDao;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;

/**
 * Created with IDEA by ZhouFy on 2018/6/22.
 *
 * @author ZhouFy
 */
public class SaveMinuteDataUtils {

    private static StatAppIdMinuteDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdMinuteDeviceActiveDao.getInstance();

    private static StatChildMinuteDeviceActiveDao statChildDeviceActiveDao = StatChildMinuteDeviceActiveDao.getInstance();

    private static StatChannelIdMinuteDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdMinuteDeviceActiveDao.getInstance();

    private static StatAppChannelIdMinuteDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdMinuteDeviceActiveDao.getInstance();

    private static StatPackageIdMinuteDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdMinuteDeviceActiveDao.getInstance();


    public static void saveMinuteDeviceInstallData(String time, int appID, int childID, int channelID, int appChannelID, int packageID) {
        /*
         *   保存 新增设备数
         */
        long childIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID);
        long childIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID);
        if (childIDCount != 0 || childIDStartupCount!= 0) {
            statChildDeviceActiveDao.saveChildMinuteDeviceInstallCount(time, appID, childID, childIDCount, childIDStartupCount);
        }

        long appIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
        long appIDStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
        if (appIDCount != 0 || appIDStartUpCount != 0) {
            statAppIdDeviceActiveDao.saveAppIdMinuteDeviceInstallCount(time , appID, appIDCount, appIDStartUpCount);
        }

        long channelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);
        long channelIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);
        if (channelIDCount != 0  || channelIDStartupCount != 0) {
            statChannelIdDeviceActiveDao.saveChannelIDMinuteDeviceInstallCount(time, appID, childID, channelID, channelIDCount, channelIDStartupCount);
        }

        long appChannelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        long appChannelIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (appChannelIDCount != 0 || appChannelIDStartupCount!= 0) {
            statAppChannelIdDeviceActiveDao.saveAppChannelIDMinuteDeviceInstallCount(time, appID, childID, channelID, appChannelID, appChannelIDCount, appChannelIDStartupCount);
        }

        long packageIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        long packageIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (packageIDCount != 0 || packageIDStartupCount != 0) {
            statPackageIdDeviceActiveDao.savePackageIDMinuteDeviceInstallCount(time, appID, childID, channelID, appChannelID, packageID, packageIDCount, packageIDStartupCount);
        }
    }
}
