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


    public static String statRedisPoolKey = "stat";

    public static String kafkaOffsetRedisPoolKey = "kafkaOffset";

    public static String iosClickPoolKey = "iosClick";

    private JedisPoolConfigInfo() {

        jedisPoolMap = new HashMap<>(16);

        int timeOut = Integer.parseInt(PropertyUtils.getValue("redis.timeout"));


        /**
         *  保存统计数据
         */
        String statHost = PropertyUtils.getValue("stat.redis.host");
        int statPort = Integer.parseInt(PropertyUtils.getValue("stat.redis.port"));
        String statPassword = PropertyUtils.getValue("stat.redis.password");

        JedisPool statRedis = new JedisPool(getJedisPoolConfig() , statHost , statPort , timeOut , statPassword);
        jedisPoolMap.put(statRedisPoolKey , statRedis);


        /**
         *   保存 kafka offset
         */
        String kafkaOffsetHost = PropertyUtils.getValue("kafka.offset.redis.host");
        int kafkaOffsetPort = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.port"));
        String kafkaOffsetPassword = PropertyUtils.getValue("kafka.offset.redis.password");
        int dataIndex = Integer.parseInt(PropertyUtils.getValue("kafka.offset.redis.dataIndex"));
        JedisPool kafkaOffsetRedis = new JedisPool(getJedisPoolConfig() , kafkaOffsetHost , kafkaOffsetPort , timeOut , kafkaOffsetPassword , dataIndex);
        jedisPoolMap.put(kafkaOffsetRedisPoolKey , kafkaOffsetRedis);


        /**
         *  保存 ios click id
         */
        String iosClickHost = PropertyUtils.getValue("redis.ios.click.redis.host");
        int iosClickPort = Integer.parseInt(PropertyUtils.getValue("redis.ios.click.redis.port"));
        String iosClickPassword = PropertyUtils.getValue("redis.ios.click.redis.password");
        int iosClickDataIndex = Integer.parseInt(PropertyUtils.getValue("redis.ios.click.redis.dataIndex"));
        JedisPool iosClickRedis = new JedisPool(getJedisPoolConfig() , iosClickHost , iosClickPort , timeOut , iosClickPassword , iosClickDataIndex);
        jedisPoolMap.put(iosClickPoolKey , iosClickRedis);



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
