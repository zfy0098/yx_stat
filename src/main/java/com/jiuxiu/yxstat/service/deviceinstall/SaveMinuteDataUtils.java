package com.jiuxiu.yxstat.service.deviceinstall;

import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatAppIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChannelIdDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatChildDeviceActiveDao;
import com.jiuxiu.yxstat.dao.stat.deviceinstall.StatPackageIdDeviceActiveDao;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.redis.deviceinstall.JedisDeviceInstallKeyConstant;

/**
 * Created with IDEA by ZhouFy on 2018/6/22.
 *
 * @author ZhouFy
 */
public class SaveMinuteDataUtils {

    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();


    public static void saveMinuteDeviceInstallData(String time, int appID, int childID, int channelID, int appChannelID, int packageID) {
        /*
         *   保存 新增设备数
         */
        long childIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID);
        long childIDStartupDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID);
        if (childIDCount != 0 || childIDStartupDeviceCount!= 0) {
            statChildDeviceActiveDao.saveChildMinuteDeviceInstallCount(time, appID, childID, childIDCount, childIDStartupDeviceCount);
        }

        long appIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
        long appIDStartUpDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
        if (appIDCount != 0 || appIDStartUpDeviceCount != 0) {
            statAppIdDeviceActiveDao.saveAppIdMinuteDeviceInstallCount(time , appID, appIDCount, appIDStartUpDeviceCount);
        }

        long channelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);
        long channelIDStartupDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);
        if (channelIDCount != 0  || channelIDStartupDeviceCount != 0) {
            statChannelIdDeviceActiveDao.saveChannelIDMinuteDeviceInstallCount(time, appID, childID, channelID, channelIDCount, channelIDStartupDeviceCount);
        }

        long appChannelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        long appChannelIDStartupDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (appChannelIDCount != 0 || appChannelIDStartupDeviceCount!= 0) {
            statAppChannelIdDeviceActiveDao.saveAppChannelIDMinuteDeviceInstallCount(time, appID, childID, channelID, appChannelID, appChannelIDCount, appChannelIDStartupDeviceCount);
        }

        long packageIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        long packageIDStartupDeviceCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, time + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (packageIDCount != 0 || packageIDStartupDeviceCount != 0) {
            statPackageIdDeviceActiveDao.savePackageIDMinuteDeviceInstallCount(time, appID, childID, channelID, appChannelID, packageID, packageIDCount, packageIDStartupDeviceCount);
        }
    }
}
