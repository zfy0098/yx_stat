package com.jiuxiu.yxstat.es.bean;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class UserBean {


    private int uid;
    private int sex;
    private String nickname;
    private String email;
    private String province;
    private String city;
    private String registtime;
    private String registchannel;
    private String subregistchannel;
    private int registplatform;
    private String phone;
    private int identity;
    private String accountid;
    private String accountname;
    private String authkey;
    private String imei;


    public UserBean(int uid, int sex, String nickname, String email, String province, String city, String registtime, String registchannel, String subregistchannel, int registplatform, String phone, int identity, String accountid, String accountname, String authkey, String imei) {
        this.uid = uid;
        this.sex = sex;
        this.nickname = nickname;
        this.email = email;
        this.province = province;
        this.city = city;
        this.registtime = registtime;
        this.registchannel = registchannel;
        this.subregistchannel = subregistchannel;
        this.registplatform = registplatform;
        this.phone = phone;
        this.identity = identity;
        this.accountid = accountid;
        this.accountname = accountname;
        this.authkey = authkey;
        this.imei = imei;
    }

    public UserBean() { }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getSex() {
        return sex;
    }

    public void setSex(int sex) {
        this.sex = sex;
    }

    public String getNickname() {
        return nickname;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public String getRegisttime() {
        return registtime;
    }

    public void setRegisttime(String registtime) {
        this.registtime = registtime;
    }

    public String getRegistchannel() {
        return registchannel;
    }

    public void setRegistchannel(String registchannel) {
        this.registchannel = registchannel;
    }

    public String getSubregistchannel() {
        return subregistchannel;
    }

    public void setSubregistchannel(String subregistchannel) {
        this.subregistchannel = subregistchannel;
    }

    public int getRegistplatform() {
        return registplatform;
    }

    public void setRegistplatform(int registplatform) {
        this.registplatform = registplatform;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public int getIdentity() {
        return identity;
    }

    public void setIdentity(int identity) {
        this.identity = identity;
    }

    public String getAccountid() {
        return accountid;
    }

    public void setAccountid(String accountid) {
        this.accountid = accountid;
    }

    public String getAccountname() {
        return accountname;
    }

    public void setAccountname(String accountname) {
        this.accountname = accountname;
    }

    public String getAuthkey() {
        return authkey;
    }

    public void setAuthkey(String authkey) {
        this.authkey = authkey;
    }

    public String getImei() {
        return imei;
    }

    public void setImei(String imei) {
        this.imei = imei;
    }
}
