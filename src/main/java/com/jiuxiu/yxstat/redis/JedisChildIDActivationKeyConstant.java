package com.jiuxiu.yxstat.redis;


public class JedisChildIDActivationKeyConstant {

    /**
     *   启动次数
     */
    public static String CHILD_ID_STARTUP_COUNT = "_CHILD_ID_STARTUP_COUNT_";

    /**
     *   没有唯一值 同ip， 设备名 系统名 新增设备数
     */
    public static String CHILD_ID_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT = "CHILD_ID_NEW_DEVICE_IP_DEVICENAME_DEVICEOSVER_COUNT:";

    /**
     *   新增设备数
     */
    public static String CHILD_ID_NEW_DEVICE_COUNT = "_CHILD_ID_NEW_DEVICE_COUNT:";

    /**
     *   启动设备数
     */
    public static String CHILD_ID_STARTUP_DEVICE_COUNT = "_CHILD_ID_STARTUP_DEVICE_COUNT:";

    /**
     *   启动设备信息
     */
    public static String CHILD_ID_STARTUP_DEVICE_INFO = "_CHILD_ID_STARTUP_DEVICE_INFO_IMEI:";


    /**
     *   ios 没有唯一值 ， 同 ip 设备名，设备系统版本 启动的设备数 key
     */
    public static String CHILD_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER = "_CHILD_ID_STARTUP_DEVCEI_INFO_IP_DEVICENAME_DEVICEOSVER=";

}
