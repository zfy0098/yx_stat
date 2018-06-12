package com.jiuxiu.yxstat.redis;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;
import java.util.Map;

/**
 * Created with IDEA by Zhoufy on 2018/5/14.
 *
 * @author Zhoufy
 */
public class JedisPoolConfigInfo extends JedisPoolConfigAbstract {


    private Map<String, JedisPool> jedisPoolMap;

    private static JedisPoolConfigInfo jedisPoolConfig = null;


    public static String userRedisPoolKey = "user";

    public static String systemRedisPoolKey = "system";

    public static String payRedisPoolKey = "pay";

    public static String configRedisPoolKey = "config";

    public static String pikaRedisPoolKey = "pika";

    public static String statRedisPoolKey = "stat";

    public static String kafkaOffsetRedisPoolKey = "kafkaOffset";


    private JedisPoolConfigInfo() {

        jedisPoolMap = new HashMap<>(16);

        int timeOut = Integer.parseInt(PropertyUtils.getValue("redis.timeout"));


        /**
         *   config redis  连接配置信息
         */
        String configHost = PropertyUtils.getValue("config.redis.host");
        int configPort = Integer.parseInt(PropertyUtils.getValue("config.redis.port"));
        String configPassword = PropertyUtils.getValue("config.redis.password");

        JedisPool configRedis = new JedisPool(getJedisPoolConfig(), configHost, configPort, timeOut, configPassword);
        jedisPoolMap.put(configRedisPoolKey, configRedis);


        /**
         *   pay redis 连接配置信息
         */
        String payHost = PropertyUtils.getValue("pay.redis.host");
        int payPort = Integer.parseInt(PropertyUtils.getValue("pay.redis.port"));
        String payPassword = PropertyUtils.getValue("pay.redis.password");

        JedisPool payRedis = new JedisPool(getJedisPoolConfig(), payHost, payPort, timeOut, payPassword);
        jedisPoolMap.put(payRedisPoolKey, payRedis);


        /**
         *   system redis  连接配置信息
         */
        String systemHost = PropertyUtils.getValue("system.redis.host");
        int systemPort = Integer.parseInt(PropertyUtils.getValue("system.redis.port"));
        String systemPassword = PropertyUtils.getValue("system.redis.password");

        JedisPool systemRedis = new JedisPool(getJedisPoolConfig(), systemHost, systemPort, timeOut, systemPassword);
        jedisPoolMap.put(systemRedisPoolKey, systemRedis);

        /**
         *   UserBean redis 配置信息
         */
        String userHost = PropertyUtils.getValue("user.redis.host");
        int userPort = Integer.parseInt(PropertyUtils.getValue("user.redis.port"));
        String userPassword = PropertyUtils.getValue("user.redis.password");

        JedisPool userRedis = new JedisPool(getJedisPoolConfig(), userHost, userPort, timeOut, userPassword);
        jedisPoolMap.put(userRedisPoolKey, userRedis);


        String statHost = PropertyUtils.getValue("stat.redis.host");
        int statPort = Integer.parseInt(PropertyUtils.getValue("stat.redis.port"));
        String statPassword = PropertyUtils.getValue("stat.redis.password");

        JedisPool statRedis = new JedisPool(getJedisPoolConfig() , statHost , statPort , timeOut , statPassword);
        jedisPoolMap.put(statRedisPoolKey , statRedis);


        /**
         *   保存 kafka offset
         */
        String kafkaOffsetHost = PropertyUtils.getValue("kafka.offset.redis.host");
        int  kafkaOffsetProt = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.port"));
        String kafkaOffsetPasswrod = PropertyUtils.getValue("kafka.offset.redis.password");
        int dataIndex = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.dataIndex"));
        JedisPool kafkaOffsetRedis = new JedisPool(getJedisPoolConfig() , kafkaOffsetHost , kafkaOffsetProt , timeOut , kafkaOffsetPasswrod , dataIndex);
        jedisPoolMap.put(kafkaOffsetRedisPoolKey , kafkaOffsetRedis);

        /**
         *   pika reids 配置信息
         */
        String pikaHost = PropertyUtils.getValue("pika.host");
        int pikaPort = Integer.parseInt(PropertyUtils.getValue("pika.port"));
        String pikaPassword = PropertyUtils.getValue("pika.password");

        JedisPool  pikaJedis = new JedisPool(getJedisPoolConfig(), pikaHost, pikaPort, timeOut, pikaPassword);
        jedisPoolMap.put(pikaRedisPoolKey , pikaJedis);

    }

    public static JedisPoolConfigInfo getJedisPool() {
        if (jedisPoolConfig == null) {
            jedisPoolConfig = new JedisPoolConfigInfo();
        }
        return jedisPoolConfig;
    }


    public Map<String, JedisPool> getJedisPoolMap() {
        return jedisPoolMap;
    }


}
