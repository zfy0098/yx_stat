package com.jiuxiu.yxstat.redis;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class JedisPoolConfigAbstract {


    private static  JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

    static {
        jedisPoolConfig.setMaxIdle(Integer.parseInt(PropertyUtils.getValue("redis.maxIdle")));
        jedisPoolConfig.setMaxTotal(Integer.parseInt(PropertyUtils.getValue("redis.maxTotal")));
        jedisPoolConfig.setTestOnBorrow(Boolean.valueOf(PropertyUtils.getValue("redis.testOnBorrow")));
    }


    protected JedisPoolConfig getJedisPoolConfig(){
        return jedisPoolConfig;
    }













}
