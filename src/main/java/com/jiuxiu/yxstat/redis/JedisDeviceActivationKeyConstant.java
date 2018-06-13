package com.jiuxiu.yxstat.redis;


public class JedisDeviceActivationKeyConstant {

    /**
     *   android 新增设备数 redis key
     */
    public static String ANDROID_NEW_DEVICE_COUNT = "_ANDROID_NEW_DEVICE_COUNT";

    /**
     *    android 设备启动数 redis key
     */
    public static String ANDROID_STARTUP_DEVICE_COUNT = "_ANDROID_STARTUP_DEVICE_COUNT";

    /**
     *    android 启动次数
     */
    public static String ANDROID_STARTUP_COUNT = "_ANDROID_STARTUP_COUNT";

    /**
     *   IOS 新增设备数 redis key
     */
    public static String IOS_NEW_DEVICE_COUNT = "_IOS_NEW_DEVICE_COUNT";

    /**
     *    IOS 设备启动数 redis key
     */
    public static String IOS_STARTUP_DEVICE_COUNT = "_IOS_STARTUP_DEVICE_COUNT";

    /**
     *    IOS 启动次数
     */
    public static String IOS_STARTUP_COUNT = "_IOS_STARTUP_COUNT";

    /**
     *   android 当天启动设备信息的key
     */
    public static String ANDROID_STARTUP_DEVICE_INFO = "_ANDROID_STARTUP_DEVICE_INFO_IMEI:";

    /**
     *   IOS 当天启动设备信息的key
     */
    public static String IOS_STARTUP_DEVICE_INFO = "_IOS_STARTUP_DEVICE_INFO_IMEI:";

    /**
     *   ios 没有唯一值 ， 同 ip 设备名，设备系统版本 启动的设备数 key
     */
    public static String IOS_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER =   "_IOS_STARTUP_DEVICE_INFO_IP_DEVICENAME_DEVICEOSVER=";


    /**
     *   IOS 没有唯一值， 同 ip 设备名 系统版本 设备激活次数
     */
    public static final String IOS_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT = "IOS_NEW_DEVICE_INSTALL_COUNT_IP:DEVICE_NAME:DEVICE_OS_VER=";

}
