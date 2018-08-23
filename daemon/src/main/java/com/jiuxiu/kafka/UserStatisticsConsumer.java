package com.jiuxiu.kafka;

import com.jiuxiu.utils.JedisUtils;
import com.jiuxiu.utils.PropertyUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IDEA by ZhouFy on 2018/6/27.
 *
 * @author ZhouFy
 */
public class UserStatisticsConsumer {

    private Logger userRegister = Logger.getLogger("user_register");
    private Logger userLogin = Logger.getLogger("user_login");

    private Logger error = Logger.getLogger("error");

    private void init() {

        JedisUtils jedisUtils = JedisUtils.getInstance();

        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.user.register.login.name");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", zkQuorum);
        properties.put("group.id", group);
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", true);

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        List<TopicPartition> list = new ArrayList<>();
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("kafka.user.register.login.partition.count"));
        for (int i = 0; i < partitionCount; i++) {
            TopicPartition p = new TopicPartition(topics, i);
            list.add(p);
        }
        consumer.assign(list);
        for (TopicPartition topicPartition : list) {
            String value = jedisUtils.get("daemon_" + topicPartition.topic() + "_" + topicPartition.partition());
            long offset = 0;
            try {
                offset = Long.valueOf(value);
            } catch (NumberFormatException e) {
                error.info("转换偏移量失败");
                System.exit(1);
            }
            consumer.seek(topicPartition, offset);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(1000);
            for (ConsumerRecord<String, String> record : records) {
                String value =  record.value();
                if(value.contains("register_type")){
                    userRegister.info(value);
                }else{
                    userLogin.info(value);
                }
                jedisUtils.set("daemon_" + record.topic() + "_" + record.partition() , "" + (record.offset()) , 0);
            }
        }
    }

    public static void main(String[] args) {
        UserStatisticsConsumer userStatisticsConsumer = new UserStatisticsConsumer();
        userStatisticsConsumer.init();
    }
}
