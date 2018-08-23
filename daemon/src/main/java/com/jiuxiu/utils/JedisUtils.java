package com.jiuxiu.utils;

import org.apache.log4j.Logger;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisException;

/**
 * Jedis Cache 工具类
 *
 * @author hadoop
 */
public class JedisUtils {

    private static Logger logger = Logger.getLogger(JedisUtils.class);

    private JedisPool jedisPool;


    private static JedisUtils jedisUtils = new JedisUtils();

    public static JedisUtils getInstance() {
        return jedisUtils;
    }


    private JedisUtils() {
        /**
         *  保存统计数据
         */
        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
        jedisPoolConfig.setMaxIdle(Integer.parseInt(PropertyUtils.getValue("redis.maxIdle")));
        jedisPoolConfig.setMaxTotal(Integer.parseInt(PropertyUtils.getValue("redis.maxTotal")));
        jedisPoolConfig.setTestOnBorrow(Boolean.valueOf(PropertyUtils.getValue("redis.testOnBorrow")));


        String kafkaOffsetHost = PropertyUtils.getValue("kafka.offset.redis.host");
        int kafkaOffsetPort = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.port"));
        int timeOut = Integer.parseInt(PropertyUtils.getValue("redis.timeout"));
        String kafkaOffsetPassword = PropertyUtils.getValue("kafka.offset.redis.password");
        int dataIndex = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.dataIndex"));

        jedisPool = new JedisPool(jedisPoolConfig, kafkaOffsetHost, kafkaOffsetPort, timeOut, kafkaOffsetPassword, dataIndex);
    }


    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public String get(String key) {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = getResource();
            if (jedis.exists(key)) {
                value = jedis.get(key);
                if (value != null && !"nil".equalsIgnoreCase(value)) {
                    return value;
                }else{
                    return null;
                }
            }
        } catch (Exception e) {
            logger.error("获取 redis 值 失败：" + e.getMessage(), e);
        } finally {
            returnResource(jedis);
        }
        return value;
    }


    /**
     * 设置缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public String set(String key, String value, int cacheSeconds) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource();
            result = jedis.set(key, value);

            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
        } catch (Exception e) {
            logger.error("写入redis 失败：" + e.getMessage(), e);
        } finally {
            returnResource(jedis);
        }
        return result;
    }


    /**
     * 获取资源
     */
    private Jedis getResource() throws JedisException {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();
        } catch (JedisException e) {
            logger.error("获取redis 资源失败：" + e.getMessage(), e);
            throw e;
        }
        return jedis;
    }

    /**
     * 归还资源
     */
    private void returnBrokenResource(Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnBrokenResource(jedis);
        }
    }

    /**
     * 释放资源
     */
    private void returnResource(Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }
}
