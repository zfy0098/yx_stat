package com.jiuxiu.yxstat.enums;

/**
 * Created with IDEA by ZhouFy on 2018/6/27.
 *
 * @author ZhouFy
 */
public enum RegisterType {

    /**
     * 游客注册
     */
    GUEST_REGISTER(1),
    /**
     * 账号注册
     */
    ACCOUNT_REGISTER(2),
    /**
     * 手机号注册
     */
    PHONE_REGISTER(3),
    /**
     * QQ 注册
     */
    QQ_REGISTER(4),
    /**
     * WX 注册
     */
    WX_REGISTER(5),

    /**
     *  其他注册
     */
    OTHER_REGISTER(6),
    /**
     * 未知
     */
    UNKNOWN(0);

    private int type;

    RegisterType(int type) {
        this.type = type;
    }


    public static RegisterType getRegisterType(int type) {
        for (RegisterType c : RegisterType.values()) {
            if (c.getType() == type) {
                return c;
            }
        }
        return RegisterType.UNKNOWN;
    }

    public int getType() {
        return type;
    }
}
