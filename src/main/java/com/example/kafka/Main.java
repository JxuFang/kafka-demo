package com.example.kafka;

import com.example.kafka.consumer.ReceiveMessage;
import com.example.kafka.producer.SendMessage;

public class Main {

    public static final String BOOTSTRAP_SERVERS = "10.182.170.225:9092";
    public static final String TOPIC = "my-topic";
    public static final String GROUP_ID = "my-group";

    public static void main(String[] args) {
        SendMessage sendMessage = new SendMessage(BOOTSTRAP_SERVERS);
        sendMessage.send(TOPIC, 0, "1111");

        ReceiveMessage receiveMessage = new ReceiveMessage(BOOTSTRAP_SERVERS, GROUP_ID);
        receiveMessage.receiveMessage(TOPIC);
    }
}