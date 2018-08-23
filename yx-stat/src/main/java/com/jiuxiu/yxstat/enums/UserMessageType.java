package com.jiuxiu.yxstat.enums;

/**
 * Created by ChouFy on 2018/6/13.
 *
 * @author ZhouFy
 */
public enum UserMessageType {

    /**
     * 用户注册
     */
    REGISTER("register"),
    /**
     * 用户登录
     */
    LOGIN("login"),
    /**
     * 游客注册
     */
    GUEST_REGISTER("guest_register"),
    /**
     * 游客登录
     */
    GUEST_LOGIN("guest_login"),
    /**
     * 未知
     */
    UNKNOWN("unknown");

    private String msgType;

    UserMessageType(String msgType) {
        this.msgType = msgType;
    }

    public static UserMessageType getMsgType(String msgType) {
        for (UserMessageType c : UserMessageType.values()) {
            if (c.getMsgType().equals(msgType)) {
                return c;
            }
        }
        return UserMessageType.UNKNOWN;
    }

    public String getMsgType() {
        return msgType;
    }
}
