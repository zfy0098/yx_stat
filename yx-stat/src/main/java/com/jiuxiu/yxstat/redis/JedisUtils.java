/**
 * Copyright &copy; 2012-2016 <a href="https://github.com/thinkgem/jeesite">JeeSite</a> All rights reserved.
 */
package com.jiuxiu.yxstat.redis;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jiuxiu.yxstat.utils.ObjectUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Jedis Cache 工具类
 *
 * @author ThinkGem
 * @version 2014-6-29
 */
public class JedisUtils {

    private static Logger logger = LoggerFactory.getLogger(JedisUtils.class);

    private static Map<String, redis.clients.jedis.JedisPool> jedisPoolMap = JedisPoolConfigInfo.getJedisPool().getJedisPoolMap();

    /**
     * 计数形式保存key
     *
     * @param redisPoolKey
     * @param key
     * @return
     */
    public static long incr(String redisPoolKey, String key) {
        long value = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            value = jedis.incr(key);
        } catch (Exception e) {
            logger.info("incr {} = {}", key, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }

        return value;
    }


    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public static String get(String redisPoolKey, String key) {
        String value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                value = jedis.get(key);
                value = StringUtils.isNotBlank(value) && !"nil".equalsIgnoreCase(value) ? value : null;
                logger.info("get {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.info("get {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public static Object getObject(String redisPoolKey, String key) {
        Object value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                value = toObject(jedis.get(getBytesKey(key)));
                logger.info("getObject {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.info("getObject {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
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
    public static String set(String redisPoolKey, String key, String value, int cacheSeconds) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.set(key, value);

            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.info("set {} = {}", key, value);
        } catch (Exception e) {
            logger.info("set {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }


    /**
     * 如果redis  key 存在 将不执行操作
     *
     * @param redisPoolKey
     * @param key
     * @param value
     * @param cacheSeconds
     * @return
     */
    public static long setNotExists(String redisPoolKey, String key, String value, int cacheSeconds) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.setnx(key, value);

            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.info("set {} = {}", key, value);
        } catch (Exception e) {
            logger.info("set {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 设置缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static String setObject(String redisPoolKey, String key, Object value, int cacheSeconds) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.set(getBytesKey(key), toBytes(value));
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.info("setObject {} = {}", key, value);
        } catch (Exception e) {
            logger.info("setObject {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 获取List缓存
     *
     * @param key 键
     * @return 值
     */
    public static List<String> getList(String redisPoolKey, String key) {
        List<String> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                value = jedis.lrange(key, 0, -1);
                logger.info("getList {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.info("getList {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 获取List缓存
     *
     * @param key 键
     * @return 值
     */
    public static List<Object> getObjectList(String redisPoolKey, String key) {
        List<Object> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                List<byte[]> list = jedis.lrange(getBytesKey(key), 0, -1);
                value = Lists.newArrayList();
                for (byte[] bs : list) {
                    value.add(toObject(bs));
                }
                logger.info("getObjectList {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.info("getObjectList {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 设置List缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static long setList(String redisPoolKey, String key, List<String> value, int cacheSeconds) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                jedis.del(key);
            }
            result = jedis.rpush(key, (String[]) value.toArray());
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.info("setList {} = {}", key, value);
        } catch (Exception e) {
            logger.info("setList {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 设置List缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static long setObjectList(String redisPoolKey, String key, List<Object> value, int cacheSeconds) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                jedis.del(key);
            }
            List<byte[]> list = Lists.newArrayList();
            for (Object o : value) {
                list.add(toBytes(o));
            }
            result = jedis.rpush(getBytesKey(key), (byte[][]) list.toArray());
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.info("setObjectList {} = {}", key, value);
        } catch (Exception e) {
            logger.info("setObjectList {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 向List缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static long listAdd(String redisPoolKey, String key, String... value) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.rpush(key, value);
            logger.info("listAdd {} = {}", key, value);
        } catch (Exception e) {
            logger.info("listAdd {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 向List缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static long listObjectAdd(String redisPoolKey, String key, Object... value) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            List<byte[]> list = Lists.newArrayList();
            for (Object o : value) {
                list.add(toBytes(o));
            }
            result = jedis.rpush(getBytesKey(key), (byte[][]) list.toArray());
            logger.info("listObjectAdd {} = {}", key, value);
        } catch (Exception e) {
            logger.info("listObjectAdd {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public static Set<String> getSet(String redisPoolKey, String key) {
        Set<String> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                value = jedis.smembers(key);
                logger.debug("getSet {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }


    /**
     * 获取 redis set长度
     *
     * @param redisPoolKey
     * @param key
     * @return
     */
    public static Long getSetScard(String redisPoolKey, String key) {
        Long value = 0L;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                value = jedis.scard(key);
                logger.debug("getSet {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }


    public static boolean getSismember(String redisPoolKey, String key , String value ){
        boolean flag = false;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);

            if (jedis.exists(key)) {
                flag = jedis.sismember(key , value);
                logger.debug("getSet {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return flag;
    }


    /**
     * 获取缓存
     *
     * @param key 键
     * @return 值
     */
    public static Set<Object> getObjectSet(String redisPoolKey, String key) {
        Set<Object> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                value = Sets.newHashSet();
                Set<byte[]> set = jedis.smembers(getBytesKey(key));
                for (byte[] bs : set) {
                    value.add(toObject(bs));
                }
                logger.debug("getObjectSet {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getObjectSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 设置Set缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static long setSet(String redisPoolKey, String key, Set<String> value, int cacheSeconds) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                jedis.del(key);
            }
            result = jedis.sadd(key, (String[]) value.toArray());
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.debug("setSet {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 设置Set缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static long setObjectSet(String redisPoolKey, String key, Set<Object> value, int cacheSeconds) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                jedis.del(key);
            }
            Set<byte[]> set = Sets.newHashSet();
            for (Object o : value) {
                set.add(toBytes(o));
            }
            result = jedis.sadd(getBytesKey(key), (byte[][]) set.toArray());
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.debug("setObjectSet {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setObjectSet {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 向Set缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static long setSetAdd(String redisPoolKey, String key, String... value) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.sadd(key, value);
            logger.debug("setSetAdd {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setSetAdd {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    public static long setSetAdd(String redisPoolKey, String key, int cacheSeconds, String... value) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.sadd(key, value);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.debug("setSetAdd {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setSetAdd {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }


    /**
     * 向Set缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static long setSetObjectAdd(String redisPoolKey, String key, Object... value) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            Set<byte[]> set = Sets.newHashSet();
            for (Object o : value) {
                set.add(toBytes(o));
            }
            result = jedis.rpush(getBytesKey(key), (byte[][]) set.toArray());
            logger.debug("setSetObjectAdd {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setSetObjectAdd {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 获取Map缓存
     *
     * @param key 键
     * @return 值
     */
    public static Map<String, String> getMap(String redisPoolKey, String key) {
        Map<String, String> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                value = jedis.hgetAll(key);
                logger.debug("getMap {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getMap {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 获取Map缓存
     *
     * @param key 键
     * @return 值
     */
    public static Map<String, Object> getObjectMap(String redisPoolKey, String key) {
        Map<String, Object> value = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                value = Maps.newHashMap();
                Map<byte[], byte[]> map = jedis.hgetAll(getBytesKey(key));
                for (Map.Entry<byte[], byte[]> e : map.entrySet()) {
                    value.put(StringUtils.toString(e.getKey()), toObject(e.getValue()));
                }
                logger.debug("getObjectMap {} = {}", key, value);
            }
        } catch (Exception e) {
            logger.warn("getObjectMap {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return value;
    }

    /**
     * 设置Map缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static String setMap(String redisPoolKey, String key, Map<String, String> value, int cacheSeconds) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                jedis.del(key);
            }
            result = jedis.hmset(key, value);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.debug("setMap {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setMap {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 设置Map缓存
     *
     * @param key          键
     * @param value        值
     * @param cacheSeconds 超时时间，0为不超时
     * @return
     */
    public static String setObjectMap(String redisPoolKey, String key, Map<String, Object> value, int cacheSeconds) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                jedis.del(key);
            }
            Map<byte[], byte[]> map = Maps.newHashMap();
            for (Map.Entry<String, Object> e : value.entrySet()) {
                map.put(getBytesKey(e.getKey()), toBytes(e.getValue()));
            }
            result = jedis.hmset(getBytesKey(key), (Map<byte[], byte[]>) map);
            if (cacheSeconds != 0) {
                jedis.expire(key, cacheSeconds);
            }
            logger.debug("setObjectMap {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("setObjectMap {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 向Map缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static String mapPut(String redisPoolKey, String key, Map<String, String> value) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.hmset(key, value);
            logger.debug("mapPut {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("mapPut {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 向Map缓存中添加值
     *
     * @param key   键
     * @param value 值
     * @return
     */
    public static String mapObjectPut(String redisPoolKey, String key, Map<String, Object> value) {
        String result = null;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            Map<byte[], byte[]> map = Maps.newHashMap();
            for (Map.Entry<String, Object> e : value.entrySet()) {
                map.put(getBytesKey(e.getKey()), toBytes(e.getValue()));
            }
            result = jedis.hmset(getBytesKey(key), (Map<byte[], byte[]>) map);
            logger.debug("mapObjectPut {} = {}", key, value);
        } catch (Exception e) {
            logger.warn("mapObjectPut {} = {}", key, value, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 移除Map缓存中的值
     *
     * @param key 键
     * @return
     */
    public static long mapRemove(String redisPoolKey, String key, String mapKey) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.hdel(key, mapKey);
            logger.debug("mapRemove {}  {}", key, mapKey);
        } catch (Exception e) {
            logger.warn("mapRemove {}  {}", key, mapKey, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 移除Map缓存中的值
     *
     * @param key 键
     * @return
     */
    public static long mapObjectRemove(String redisPoolKey, String key, String mapKey) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.hdel(getBytesKey(key), getBytesKey(mapKey));
            logger.debug("mapObjectRemove {}  {}", key, mapKey);
        } catch (Exception e) {
            logger.warn("mapObjectRemove {}  {}", key, mapKey, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 判断Map缓存中的Key是否存在
     *
     * @param key 键
     * @return
     */
    public static boolean mapExists(String redisPoolKey, String key, String mapKey) {
        boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.hexists(key, mapKey);
            logger.debug("mapExists {}  {}", key, mapKey);
        } catch (Exception e) {
            logger.warn("mapExists {}  {}", key, mapKey, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 判断Map缓存中的Key是否存在
     *
     * @return
     */
    public static boolean mapObjectExists(String redisPoolKey, String key, String mapKey) {
        boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.hexists(getBytesKey(key), getBytesKey(mapKey));
            logger.debug("mapObjectExists {}  {}", key, mapKey);
        } catch (Exception e) {
            logger.warn("mapObjectExists {}  {}", key, mapKey, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 删除缓存
     *
     * @param key 键
     * @return
     */
    public static long del(String redisPoolKey, String key) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(key)) {
                result = jedis.del(key);
                logger.debug("del {}", key);
            } else {
                logger.debug("del {} not exists", key);
            }
        } catch (Exception e) {
            logger.warn("del {}", key, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 删除缓存
     *
     * @param key 键
     * @return
     */
    public static long delObject(String redisPoolKey, String key) {
        long result = 0;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            if (jedis.exists(getBytesKey(key))) {
                result = jedis.del(getBytesKey(key));
                logger.debug("delObject {}", key);
            } else {
                logger.debug("delObject {} not exists", key);
            }
        } catch (Exception e) {
            logger.warn("delObject {}", key, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 缓存是否存在
     *
     * @return
     */
    public static boolean exists(String redisPoolKey, String key) {
        boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.exists(key);
            logger.debug("exists {}", key);
        } catch (Exception e) {
            logger.warn("exists {}", key, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 缓存是否存在
     *
     * @param key 键
     */
    public static boolean existsObject(String redisPoolKey, String key) {
        boolean result = false;
        Jedis jedis = null;
        try {
            jedis = getResource(redisPoolKey);
            result = jedis.exists(getBytesKey(key));
            logger.debug("existsObject {}", key);
        } catch (Exception e) {
            logger.warn("existsObject {}", key, e);
        } finally {
            returnResource(redisPoolKey, jedis);
        }
        return result;
    }

    /**
     * 获取资源
     */
    public static Jedis getResource(String redisPoolKey) throws JedisException {
        Jedis jedis = null;
        try {
            jedis = jedisPoolMap.get(redisPoolKey).getResource();
        } catch (JedisException e) {
            logger.warn("getResource.", e);
            returnBrokenResource(redisPoolKey, jedis);
            throw e;
        }
        return jedis;
    }

    /**
     * 归还资源
     */
    private static void returnBrokenResource(String redisPoolKey, Jedis jedis) {
        if (jedis != null) {
            jedisPoolMap.get(redisPoolKey).returnBrokenResource(jedis);
        }
    }

    /**
     * 释放资源
     */
    public static void returnResource(String redisPoolKey, Jedis jedis) {
        if (jedis != null) {
            jedisPoolMap.get(redisPoolKey).returnResource(jedis);
        }
    }

    /**
     * 获取byte[]类型Key
     */
    public static byte[] getBytesKey(Object object) {
        if (object instanceof String) {
            return StringUtils.getBytes((String) object);
        } else {
            return ObjectUtils.serialize(object);
        }
    }

    /**
     * 获取byte[]类型Key
     */
    public static Object getObjectKey(byte[] key) {
        try {
            return StringUtils.toString(key);
        } catch (UnsupportedOperationException uoe) {
            try {
                return JedisUtils.toObject(key);
            } catch (UnsupportedOperationException uoe2) {
                uoe2.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Object转换byte[]类型
     */
    public static byte[] toBytes(Object object) {
        return ObjectUtils.serialize(object);
    }

    /**
     * byte[]型转换Object
     */
    public static Object toObject(byte[] bytes) {
        return ObjectUtils.unserialize(bytes);
    }

}
