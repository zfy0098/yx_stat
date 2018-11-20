package com.jiuxiu.yxstat.redis;

import com.jiuxiu.yxstat.utils.PropertyUtils;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
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

    public static String adClickPoolKey = "adClick";

    public static String appRolePoolKey = "appRole";

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
        String adClickHost = PropertyUtils.getValue("redis.ad.click.redis.host");
        int adClickPort = Integer.parseInt(PropertyUtils.getValue("redis.ad.click.redis.port"));
        String adClickPassword = PropertyUtils.getValue("redis.ad.click.redis.password");
        int adClickDataIndex = Integer.parseInt(PropertyUtils.getValue("redis.ad.click.redis.dataIndex"));
        JedisPool adClickRedis = new JedisPool(getJedisPoolConfig() , adClickHost , adClickPort , timeOut , adClickPassword , adClickDataIndex);
        jedisPoolMap.put(adClickPoolKey, adClickRedis);



        String appRoleHost = PropertyUtils.getValue("redis.app_role.host");
        int appRolePort = Integer.parseInt(PropertyUtils.getValue("redis.app_role.port"));
        String appRolePassword = PropertyUtils.getValue("redis.app_role.password");
        int appRoleDataIndex = Integer.parseInt(PropertyUtils.getValue("redis.app_role.dateIndex"));
        JedisPool appRole = new JedisPool(getJedisPoolConfig(), appRoleHost, appRolePort, timeOut, appRolePassword, appRoleDataIndex);
        jedisPoolMap.put(appRolePoolKey, appRole);



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
