package com.jiuxiu.yxstat.redis.deviceinstall;

/**
 * Created with IDEA by ZhouFy on 2018/6/22.
 *
 * @author ZhouFy
 */
public class JedisDeviceInstallKeyConstant {


    /**
     * child id 新增设备数
     */
    public static String CHILD_ID_NEW_DEVICE_COUNT = "_CHILD_ID_NEW_DEVICE_COUNT:";

    /**
     * child id 启动设备数
     */
    public static String CHILD_ID_STARTUP_DEVICE_COUNT = "_CHILD_ID_STARTUP_DEVICE_COUNT:";

    /**
     * child id  ios 没有唯一值 ， 同 ip 设备名，设备系统版本 启动的设备数 key
     */
    public static String CHILD_ID_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER = "_CHILD_ID_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER:";

    /**
     * app id 新增设备数
     */
    public static String APP_ID_NEW_DEVICE_COUNT = "_APP_ID_NEW_DEVICE_COUNT:";

    /**
     * app id 启动设备数
     */
    public static String APP_ID_STARTUP_DEVICE_COUNT = "_APP_ID_STARTUP_DEVICE_COUNT:";

    /**
     * channel id  新增设备数
     */
    public static String CHANNEL_ID_NEW_DEVICE_COUNT = "_CHANNEL_ID_NEW_DEVICE_COUNT:";

    /**
     * channel id  启动设备数
     */
    public static String CHANNEL_ID_STARTUP_DEVICE_COUNT = "_CHANNEL_ID_STARTUP_DEVICE_COUNT:";

    /**
     * app channel id 新增设备数
     */
    public static String APP_CHANNEL_ID_NEW_DEVICE_COUNT = "_APP_CHANNEL_ID_NEW_DEVICE_COUNT:";

    /**
     * app channel id 启动设备数
     */
    public static String APP_CHANNEL_ID_STARTUP_DEVICE_COUNT = "_APP_CHANNEL_ID_STARTUP_DEVICE_COUNT:";

    /**
     * package id  新增设备数
     */
    public static String PACKAGE_ID_NEW_DEVICE_COUNT = "_PACKAGE_ID_NEW_DEVICE_COUNT:";

    /**
     * package id  启动设备数
     */
    public static String PACKAGE_ID_STARTUP_DEVICE_COUNT = "_PACKAGE_ID_STARTUP_DEVICE_COUNT:";


}
