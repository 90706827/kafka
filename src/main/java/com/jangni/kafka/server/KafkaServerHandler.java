package com.jangni.kafka.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaServerHandler {


    public void proc(ConsumerRecord<String, String> record) {
        String reqMsg = record.value();
        System.out.println("接收到kafka消息：" + reqMsg);
        System.out.println("继续后续业务处理....");
    }
}
