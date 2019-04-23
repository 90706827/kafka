package com.jangni.kafka.server;

import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @program: java
 * @description: kafka监听线程
 * @author: Mr.Jangni
 * @create: 2018-09-07 16:54
 **/
public class MonitorRunnable implements Runnable {
    protected Logger logger = LoggerFactory.getLogger(MonitorRunnable.class);
    protected KafkaConsumer<String, String> consumer;
    protected LinkedBlockingQueue<String> queue;
    protected List<String> topics;
    protected Long poolMillions;

    @Override
    public void run() {
        while (!Thread.interrupted()) {
            //读取数据
            ConsumerRecords<String, String> records = consumer.poll(poolMillions);
            Set<TopicPartition> assignmentTopicPartitions = consumer.assignment();
            //获取当前topic的读取的partition
//            String asTopicParMsg =  assignmentTopicPartitions.iterator();
        }
    }
}
