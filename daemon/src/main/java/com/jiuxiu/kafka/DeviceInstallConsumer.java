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
 * Created with IDEA by ZhouFy on 2018/6/26.
 *
 * @author ZhouFy
 */
public class DeviceInstallConsumer {

    private Logger log = Logger.getLogger(this.getClass());

    public void init() {

        JedisUtils jedisUtils = JedisUtils.getInstance();

        String zkQuorum = PropertyUtils.getValue("kafka.bootstrap.servers");
        String group = PropertyUtils.getValue("kafka.group.id");
        String topics = PropertyUtils.getValue("kafka.device.install.topics");

        Properties properties = new Properties();
        properties.put("bootstrap.servers", zkQuorum);
        properties.put("group.id", group);
        properties.put("auto.offset.reset", "earliest");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("enable.auto.commit", false);

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        List<TopicPartition> list = new ArrayList<>();
        int partitionCount = Integer.parseInt(PropertyUtils.getValue("kafka.device.install.partition.count"));

        for (int i = 0; i < partitionCount; i++) {
            TopicPartition p = new TopicPartition(topics, i);
            list.add(p);
        }
        kafkaConsumer.assign(list);

        for (TopicPartition topicPartition : list) {
            String value = jedisUtils.get("daemon_" + topicPartition.topic() + "_" + topicPartition.partition());
            long offset = 0;
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
                log.info(record.value());
                jedisUtils.set("daemon_" + record.topic() + "_" + record.partition(), "" + (record.offset()) , 0);
            }
        }
    }

    public static void main(String[] args) {
        DeviceInstallConsumer deviceInstallConsumer = new DeviceInstallConsumer();
        deviceInstallConsumer.init();
    }
}
