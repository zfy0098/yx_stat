package com.jiuxiu.yxstat.redis;

/**
 * Created by ZhouFy on 2018/6/13.
 *
 * @author ZhouFy
 */
public class JedisUserStatisticsKeyConstant {

    /**
     *   用户注册数量
     */
    public static String USER_REGISTER_COUNT = "_USER_REGISTER_COUNT:";

    /**
     *   数量登录数量
     */
    public static String USER_LOGIN_COUNT = "_USER_LOGIN_COUNT:";

    /**
     *   登录用户 信息
     */
    public static String USER_LOGIN_INFO = "_USER_LOGIN_INFO_UID:";

    /**
     *  游客注册数量
     */
    public static String GUEST_USER_REGISTER_COUNT = "_GUEGET_USER_REGISTER_COUNT:";

    /**
     *   游客登录数量
     */
    public static String GUEST_USER_LOGIN_COUNT = "_GUEST_USER_LOGIN_COUNT:";

    /**
     *   游客登录信息
     */
    public static String GUEST_USER_LOGIN_INFO = "_GUEST_USER_LOGIN_INFO_UID:";
}
