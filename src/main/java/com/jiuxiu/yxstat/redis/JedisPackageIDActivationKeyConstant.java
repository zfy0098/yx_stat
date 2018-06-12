package com.jiuxiu.yxstat.redis;

public class JedisPackageIDActivationKeyConstant {

    /**
     *   启动次数
     */
    public static String PACKAGE_ID_STARTUP_COUNT = "_PACKAGE_ID_STARTUP_COUNT_";

    /**
     *   没有唯一值 同ip， 设备名 系统名 新增设备数
     */
    public static String PACKAGE_ID_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT = "PACKAGE_ID_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT:";

    /**
     *   新增设备数
     */
    public static String PACKAGE_ID_NEW_DEVICE_COUNT = "_PACKAGE_ID_NEW_DEVICE_COUNT:";

    /**
     *   启动设备数
     */
    public static String PACKAGE_ID_STARTUP_DEVICE_COUNT = "_PACKAGE_ID_STARTUP_DEVICE_COUNT:";

    /**
     *   IOS 启动设备数
     */
    public static String IOS_PACKAGE_ID_STARTUP_DEVICE_COUNT = "_IOS_PACKAGE_ID_STARTUP_DEVICE_COUNT:";

    /**
     *   启动设备信息
     */
    public static String PACKAGE_ID_STARTUP_DEVICE_INFO = "_PACKAGE_ID_STARTUP_DEVICE_INFO_IMEI:";

    /**
     * ios 启动设备信息
     */
    public static String IOS_PACKAGE_ID_STARTUP_DEVICE_INFO = "_IOS_PACKAGE_ID_STARTUP_DEVICE_INFO_IDFA:";

    /**
     *   ios 没有唯一值 ， 同 ip 设备名，设备系统版本 启动的设备数 key
     */
    public static String PACKAGE_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER = "_APP_PACKAGE_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER=";

}
