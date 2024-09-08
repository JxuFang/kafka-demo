package com.example.kafka.producer;

import com.example.kafka.utils.Utils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class SendMessage {

    private final String bootstrapServers;

    public SendMessage(String boostrapServer) {
        this.bootstrapServers = boostrapServer;
    }

    public void send(String topic, Integer key, String value) {
        try (KafkaProducer<Integer, String> kafkaProducer = createKafkaProducer()){
            ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, key, value);
            kafkaProducer.send(record);
        } catch (Exception e) {
            Utils.printErr("Unhandled exception: " + e);
        }
    }

    private KafkaProducer<Integer, String> createKafkaProducer() {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducer<>(props);
    }
}
