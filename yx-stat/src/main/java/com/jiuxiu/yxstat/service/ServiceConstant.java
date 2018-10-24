package com.jiuxiu.yxstat.service;


/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class ServiceConstant {

    /**
     * redis 一年失效
     */
    public static final int REDIS_EXPIRE_TIME_YEARS = 60 * 60 * 24 * 365;

    /**
     * 设备激活信息有效期
     */
    public static final int DEVICE_ACTIVATION_EXPIRE_TIME = 60 * 60 * 24 * 7;

    /**
     *   激活信息存储时间
     */
    public static final int ACTIVATION_INFO_EXPIRE_TIME = 60 * 60;

    /**
     * 登录用户信息有效期 90天
     */
    public static final int USER_INFO_REDIS_EXPIRE_TIME = 60 * 60 * 24 * 90;

    /**
     *   保存分钟 用户数据的 redis 有效期
     */
    public static final int USER_INFO_MINUTE_REDIS_EXPIRE_TIME = 60 * 60 * 24 * 7;

    /**
     *   充值信息 redis 有效期
     */
    public static final int PAY_ORDER_INFO_REDIS_EXPIRE_TIME = 60 * 60 * 24 * 7;

    /**
     * 没有idfa imei 等唯一值， 同ip  同设备名 同系统版本  最大激活数
     */
    public static final int ACTIVE_COUNT = 5;

    /**
     * android 系统标识
     */
    public static final int ANDROID_OS = 1;

    /**
     * ios系统标识
     */
    public static final int IOS_OS = 2;

}
