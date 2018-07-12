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
public class SaveDataUtils {


    private static StatAppIdDeviceActiveDao statAppIdDeviceActiveDao = StatAppIdDeviceActiveDao.getInstance();

    private static StatChildDeviceActiveDao statChildDeviceActiveDao = StatChildDeviceActiveDao.getInstance();

    private static StatChannelIdDeviceActiveDao statChannelIdDeviceActiveDao = StatChannelIdDeviceActiveDao.getInstance();

    private static StatAppChannelIdDeviceActiveDao statAppChannelIdDeviceActiveDao = StatAppChannelIdDeviceActiveDao.getInstance();

    private static StatPackageIdDeviceActiveDao statPackageIdDeviceActiveDao = StatPackageIdDeviceActiveDao.getInstance();


    public static void saveDeviceInstallData(String toDay , int appID , int childID ,  int channelID  , int appChannelID , int packageID){

        //  child id 新增设备数
        long childIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_NEW_DEVICE_COUNT + childID + ":" + appID);
        //  child id 启动设备数
        long childIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHILD_ID_STARTUP_DEVICE_COUNT + childID + ":" + appID);
        //  将child id 新增设备数 或启动设备数保存到数据中
        if (childIDCount != 0 || childIDStartupCount != 0) {
            statChildDeviceActiveDao.saveChildIdDeviceInstallCount(toDay , appID, childID, childIDCount ,  childIDStartupCount);
        }

        //  app id 新增设备数
        long appIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_NEW_DEVICE_COUNT + appID);
        //  app id 启动设备数
        long appIDStartUpCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_ID_STARTUP_DEVICE_COUNT + appID);
        if (appIDCount != 0 || appIDStartUpCount != 0) {
            statAppIdDeviceActiveDao.saveAppIdDeviceInstallCount(toDay, appID, appIDCount, appIDStartUpCount);
        }

        // channel id 新增设备数
        long channelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_NEW_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);
        // channel id 启动设备数
        long channelIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.CHANNEL_ID_STARTUP_DEVICE_COUNT + channelID + ":" + childID + ":" + appID);

        if (channelIDCount != 0 || channelIDStartupCount != 0 ) {
            statChannelIdDeviceActiveDao.saveChannelIdDeviceInstallCount(toDay , appID, childID, channelID, channelIDCount, channelIDStartupCount);
        }

        //  app channel id 新增设备数
        long appChannelIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_NEW_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        //  app channel id 启动设备数
        long appChannelIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.APP_CHANNEL_ID_STARTUP_DEVICE_COUNT + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (appChannelIDCount != 0 || appChannelIDStartupCount!=0) {
            statAppChannelIdDeviceActiveDao.saveAppChannelIdDeviceInstallCount(toDay, appID, childID, channelID, appChannelID, appChannelIDCount, appChannelIDStartupCount);
        }

        //  package id 新增设备数
        long packageIDCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_NEW_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        //  package id 启动设备数
        long packageIDStartupCount = JedisUtils.getSetScard(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisDeviceInstallKeyConstant.PACKAGE_ID_STARTUP_DEVICE_COUNT + packageID + ":" + appChannelID + ":" + channelID + ":" + childID + ":" + appID);
        if (packageIDCount != 0 || packageIDStartupCount != 0) {
            statPackageIdDeviceActiveDao.savePackageIdDeviceInstallCount(toDay, appID, childID, channelID, appChannelID, packageID, packageIDCount,packageIDStartupCount);
        }
    }
}
