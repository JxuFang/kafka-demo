package com.example.kafka.consumer;

import com.example.kafka.utils.Utils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singleton;

public class ReceiveMessage {

    public String boostrapServer;
    public String groupId;

    public ReceiveMessage(String boostrapServer, String groupId) {
        this.boostrapServer = boostrapServer;
        this.groupId = groupId;
    }

    public void receiveMessage(String topic) {
        try (KafkaConsumer<Integer, String> kafkaConsumer = createKafkaConsumer()) {
            kafkaConsumer.subscribe(singleton(topic));
            ConsumerRecords<Integer, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                Utils.maybePrintRecord(1, record);
            }
        } catch (Exception e) {
            Utils.printErr("Unhandled exception: " + e);
        }
    }

    private KafkaConsumer<Integer, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.boostrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
        return new KafkaConsumer<>(props);
    }
}
