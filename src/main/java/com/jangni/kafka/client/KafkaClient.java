package com.jangni.kafka.client;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @program: java
 * @description: 客户端
 * @author: Mr.Jangni
 * @create: 2018-09-07 09:33
 **/
public class KafkaClient {
    protected Logger logger = LoggerFactory.getLogger(KafkaClient.class);
    private String maxBlockMs = "5000";
    private String reqTimeoutMs = "15000";
    private String valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    private String bootstrapServers = "127.0.0.1:50001,127.0.0.1:50002";
    private String acks = "all";
    private String retries = "0";
    private String batchSize = "16384";
    private String lingerMs = "5";
    private String bufferMemory = "33554432";

    private KafkaProducer<String, String> kafkaProducer;

    public KafkaClient(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        init();
    }

    /**
     * 初始化
     */
    private void init() {
        Properties props = new Properties();
        //缓冲区满、topic元数据无效 是send方法和partitionsFor方法阻塞的时间
        props.setProperty("max.block.ms", maxBlockMs);
        //请求超时时间 客户端等待一个请求的最大时间，配置的值不能小于broker的replica.lag.time.max.ms值(默认是10000)
        props.setProperty("request.timeout.ms", reqTimeoutMs);
        //value的序列化
        props.setProperty("value.serializer", valueSerializer);
        //key的序列化
        props.setProperty("key.serializer", keySerializer);
        //kafka集群地址
        props.setProperty("bootstrap.servers", bootstrapServers);
        //当一次发送消息请求被认为完成时的确认值，就是指procuder需要多少个broker返回的确认信号
        // all 表示需要等待所有的备份都成功写入日志
        props.setProperty("acks", acks);
        //如果将retries的值大于0，客户端将重新发送之前发送失败的数据
        props.setProperty("retries", retries);
        //只要有多个记录被发送到同一个分区，生产者就会尝试将记录组合成一个batch的请求；一个batch的请求大小默认值是16384
        props.setProperty("batch.size", batchSize);
        //延迟时间：为每次发送增加一些延迟，以此来聚合更多的Message，单位毫秒
        props.setProperty("linger.ms", lingerMs);
        //缓冲区大小：设置生产者可用于缓冲等待发送到服务器的记录的总字节数。
        props.setProperty("buffer.memory", bufferMemory);
        kafkaProducer = new KafkaProducer<String, String>(props);
    }

    /**
     * 发送消息
     *
     * @param msg
     */
    public void push(String topic, String msg) {
        kafkaProducer.send(new ProducerRecord<String, String>(topic, msg), new Callback() {

            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    logger.info("send kafka success");
                } else {
                    logger.error("send kafka failed", e);
                }
            }
        });
        kafkaProducer.flush();
    }

}
