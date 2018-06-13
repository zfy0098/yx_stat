package com.jiuxiu.yxstat.service.user;

import com.jiuxiu.yxstat.dao.stat.UserStatisticsDao;
import com.jiuxiu.yxstat.enums.UserMessageType;
import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUserStatisticsKeyConstant;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.service.ServiceConstant;
import com.jiuxiu.yxstat.spark.SparkConfiguration;
import com.jiuxiu.yxstat.utils.DateUtil;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import com.jiuxiu.yxstat.utils.StringUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.OffsetRange;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by ZhouFy on 2018/6/11.
 *
 * @author ZhouFy
 */
public class UserStatisticsDataService implements Serializable {

    private UserStatisticsDao userStatisticsDao = UserStatisticsDao.getInstance();

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private void userStatisticsData(String[] args) {

        String sparkMasterURL = PropertyUtils.getValue("spark.master.url");
        sparkMasterURL = "local[1]";
        String appName = "ceShi";
        int seconds = 10;
        String topics = PropertyUtils.getValue("kafka.topic.user.name");

        //  确定kafka分区数量
        int partitionCount = ServiceConstant.USER_STATISTICS_PARTITION_COUNT;
        if (args.length > 0) {
            partitionCount = Integer.parseInt(args[0]);
        }

        //  设置没个分区的 offset 信息，从redis中获取保存的offset 如果获取失败 将默认为0
        Map<TopicPartition, Long> topicPartitionMap = new HashMap<>(8);
        for (int i = 0; i < partitionCount; i++) {
            long offset;
            try {
                offset = Long.parseLong(JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + i));
            } catch (NumberFormatException e) {
                offset = 0L;
            }
            topicPartitionMap.put(new TopicPartition(topics, i), offset);
        }

        //  创建streaming 对象
        JavaStreamingContext ssc = SparkConfiguration.getJavaStreamingContext(sparkMasterURL, appName, seconds);

        //通过KafkaUtils.createDirectStream(...)获得kafka数据，kafka相关参数由kafkaParams指定
        JavaDStream<ConsumerRecord<String, String>> stream = SparkConfiguration.initialization(ssc, topicPartitionMap);

        stream.foreachRDD(new VoidFunction<JavaRDD<ConsumerRecord<String, String>>>() {
            @Override
            public void call(JavaRDD<ConsumerRecord<String, String>> consumer) {

                String toDay = DateUtil.getNowDate(DateUtil.yyyy_MM_dd);

                JavaRDD<JSONObject> javaRDD = consumer.flatMap(new FlatMapFunction<ConsumerRecord<String, String>, JSONObject>() {
                    @Override
                    public Iterator<JSONObject> call(ConsumerRecord<String, String> consumerRecord) {
                        List<JSONObject> list = new ArrayList<>();
                        list.add(JSONObject.fromObject(consumerRecord.value()));
                        return list.iterator();
                    }
                });
                javaRDD.filter(new Function<JSONObject, Boolean>() {
                    Map<String, JSONObject> map = new HashMap<>(16);

                    @Override
                    public Boolean call(JSONObject json) {
                        String key = json.getString("uid") + json.getString("msg_type");
                        if (map.get(key) == null) {
                            map.put(key, json);
                            return true;
                        }
                        return false;
                    }
                }).foreach(new VoidFunction<JSONObject>() {
                    @Override
                    public void call(JSONObject json) {
                        String msgType = json.getString("msg_type");
                        String uid = json.getString("uid");
                        UserMessageType userMessageType = UserMessageType.getMsgType(msgType);
                        switch (userMessageType) {
                            case REGISTER:
                                // 用户注册
                                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_REGISTER_COUNT);
                                break;
                            case LOGIN:
                                // 用户登录
                                String value = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_LOGIN_INFO + uid);
                                if (StringUtils.isEmpty(value)) {
                                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_LOGIN_COUNT);
                                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_LOGIN_INFO + uid, json.toString(),
                                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME);
                                }
                                break;
                            case GUEST_REGISTER:
                                // 游客注册
                                JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_REGISTER_COUNT);
                                break;
                            case GUEST_LOGIN:
                                // 游客登录
                                String guestValue = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_LOGIN_INFO + uid);
                                if (StringUtils.isEmpty(guestValue)) {
                                    JedisUtils.incr(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_LOGIN_COUNT);
                                    JedisUtils.set(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_LOGIN_INFO + uid, json.toString(),
                                            ServiceConstant.USER_INFO_REDIS_EXPIRE_TIME);
                                }
                                break;
                            default:
                                break;
                        }
                    }
                });
                //   用户注册数量
                String registerCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_REGISTER_COUNT);
                // 用户登录数量
                String loginCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.USER_LOGIN_COUNT);
                // 游客注册数量
                String guestRegisterCount = JedisUtils.get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_REGISTER_COUNT);
                // 游客登录数量
                String guestLoginCount = JedisUtils. get(JedisPoolConfigInfo.statRedisPoolKey, toDay + JedisUserStatisticsKeyConstant.GUEST_USER_LOGIN_COUNT);
                String zero = "0";
                // 保存数据库
                if (!StringUtils.equals(zero, registerCount) || !StringUtils.equals(zero, loginCount) || !StringUtils.equals(zero, guestLoginCount)
                        || !StringUtils.equals(zero, guestRegisterCount)) {
                    userStatisticsDao.saveUserStatistics(new Object[]{loginCount, registerCount, guestLoginCount, guestRegisterCount,
                            loginCount, registerCount, guestLoginCount, guestRegisterCount});
                }

                //遍历分区信息,将新的offset保存到redis中
                OffsetRange[] offsetRanges = ((HasOffsetRanges) consumer.rdd()).offsetRanges();
                consumer.foreach(x -> {
                    log.info(x.key() + "---------------" + x.value());
                });
                for (int i = 0; i < offsetRanges.length; i++) {
                    log.info("输出 offsetRanges ：" + offsetRanges[i]);
                    JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, topics + "_" + offsetRanges[i].partition(), String.valueOf(offsetRanges[i].fromOffset()), 0);
                }
            }
        });
    }

    public static void main(String[] args) {
        UserStatisticsDataService userStatisticsDataService = new UserStatisticsDataService();
        userStatisticsDataService.userStatisticsData(args);
    }
}
