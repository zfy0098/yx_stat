package com.jiuxiu.yxstat.service.approle;

import com.jiuxiu.yxstat.redis.JedisPoolConfigInfo;
import com.jiuxiu.yxstat.redis.JedisUtils;
import com.jiuxiu.yxstat.utils.PropertyUtils;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created with IDEA by ChouFy on 2018/11/14.
 *
 * @author Zhoufy
 */
public class AppRoleDataService {


    private void run(){


        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.app.role.topics");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", zkQuorum);
        properties.put("group.id", group);
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        List<TopicPartition> list = new ArrayList<>();
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("kafka.app.role.partition.count"));

        for (int i = 0; i < partitionCount; i++) {
            TopicPartition p = new TopicPartition(topics, i);
            list.add(p);
        }
        kafkaConsumer.assign(list);

        for (TopicPartition topicPartition : list) {
            String value = JedisUtils.get(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey , topicPartition.topic() + "_" + topicPartition.partition());
            long offset;
            try {
                offset = Long.valueOf(value);
            } catch (NumberFormatException e) {
                offset = 0;
            }
            kafkaConsumer.seek(topicPartition, offset);
        }


        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(1000);


            for (ConsumerRecord<String, String> record : records) {

                String value = record.value();
                JSONObject json = JSONObject.fromObject(value);

                String appid = json.getString("appid");
                String childID = json.getString("child_id");
                String channelID = json.getString("channel_id");
                String appChannelID = json.getString("acid");
                String packageID = json.getString("package_id");


                JedisUtils.set(JedisPoolConfigInfo.kafkaOffsetRedisPoolKey, record.topic() + "_" + record.partition(), "" + (record.offset()) , 0);
            }


            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    public static void main(String[] args) {
        AppRoleDataService appRoleDataService = new AppRoleDataService();
        appRoleDataService.run();
    }
}
