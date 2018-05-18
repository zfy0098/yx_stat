package com.jiuxiu.yxstat.es.bean;

import java.io.Serializable;

/**
 * Created with IDEA by Zhoufy on 2018/5/16.
 *
 * @author Zhoufy
 */
public class UserLoginBean implements Serializable{



    private int uid;
    private String lastloginip;
    private int lastlogintime;
    private int logintimes;
    private int loginplatform;
    private String loginimei;
    private String lastlogoutip;
    private String lastlogoutime;
    private String appid;
    private String device_name;
    private String device_os_ver;
    private String bundleid;
    private String version_name;
    private String channel;
    private String networking;


    public UserLoginBean() { }

    public UserLoginBean(int uid, String lastloginip, int lastlogintime, int logintimes, int loginplatform, String loginimei, String lastlogoutip, String lastlogoutime, String appid, String device_name, String device_os_ver, String bundleid, String version_name, String channel, String networking) {
        this.uid = uid;
        this.lastloginip = lastloginip;
        this.lastlogintime = lastlogintime;
        this.logintimes = logintimes;
        this.loginplatform = loginplatform;
        this.loginimei = loginimei;
        this.lastlogoutip = lastlogoutip;
        this.lastlogoutime = lastlogoutime;
        this.appid = appid;
        this.device_name = device_name;
        this.device_os_ver = device_os_ver;
        this.bundleid = bundleid;
        this.version_name = version_name;
        this.channel = channel;
        this.networking = networking;
    }


    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public String getLastloginip() {
        return lastloginip;
    }

    public void setLastloginip(String lastloginip) {
        this.lastloginip = lastloginip;
    }

    public int getLastlogintime() {
        return lastlogintime;
    }

    public void setLastlogintime(int lastlogintime) {
        this.lastlogintime = lastlogintime;
    }

    public int getLogintimes() {
        return logintimes;
    }

    public void setLogintimes(int logintimes) {
        this.logintimes = logintimes;
    }

    public int getLoginplatform() {
        return loginplatform;
    }

    public void setLoginplatform(int loginplatform) {
        this.loginplatform = loginplatform;
    }

    public String getLoginimei() {
        return loginimei;
    }

    public void setLoginimei(String loginimei) {
        this.loginimei = loginimei;
    }

    public String getLastlogoutip() {
        return lastlogoutip;
    }

    public void setLastlogoutip(String lastlogoutip) {
        this.lastlogoutip = lastlogoutip;
    }

    public String getLastlogoutime() {
        return lastlogoutime;
    }

    public void setLastlogoutime(String lastlogoutime) {
        this.lastlogoutime = lastlogoutime;
    }

    public String getAppid() {
        return appid;
    }

    public void setAppid(String appid) {
        this.appid = appid;
    }

    public String getDevice_name() {
        return device_name;
    }

    public void setDevice_name(String device_name) {
        this.device_name = device_name;
    }

    public String getDevice_os_ver() {
        return device_os_ver;
    }

    public void setDevice_os_ver(String device_os_ver) {
        this.device_os_ver = device_os_ver;
    }

    public String getBundleid() {
        return bundleid;
    }

    public void setBundleid(String bundleid) {
        this.bundleid = bundleid;
    }

    public String getVersion_name() {
        return version_name;
    }

    public void setVersion_name(String version_name) {
        this.version_name = version_name;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getNetworking() {
        return networking;
    }

    public void setNetworking(String networking) {
        this.networking = networking;
    }
}
