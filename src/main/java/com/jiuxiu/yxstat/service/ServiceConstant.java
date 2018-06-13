package com.jiuxiu.yxstat.service;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class ServiceConstant {

    /**
     *   redis 一年失效
     */
    public static final int REDIS_EXPIRE_TIME_YEARS = 60 * 60 * 24 * 365;

    /**
     *   登录用户信息有效期 90天
     */
    public static final int USER_INFO_REDIS_EXPIRE_TIME = 60 * 60 * 24 * 90;

    /**
     *    没有idfa imei 等唯一值， 同ip  同设备名 同系统版本  最大激活数
     */
    public static final int ACTIVE_COUNT = 5;

    /**
     *    kafka 设备激活 topic 分区数量
     */
    public static final int DEVICE_INSTALL_PARTITION_COUNT = 2;

    /**
     *   kafka 用户登录信息 topic 分区数量
     */
    public static final int USER_STATISTICS_PARTITION_COUNT = 2;

    /**
     *   android 系统标识
     */
    public static final int ANDROID_OS = 1;

    /**
     *   ios系统标识
     */
    public static final int IOS_OS = 2;


}
