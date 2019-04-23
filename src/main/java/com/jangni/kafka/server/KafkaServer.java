package com.jangni.kafka.server;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @program: java
 * @description: 消费者
 * @author: Mr.Jangni
 * @create: 2018-09-07 14:00
 **/
public class KafkaServer {
    protected Logger logger = LoggerFactory.getLogger(KafkaServer.class);
    protected String bootstrapServers = "127.0.0.1:50001,127.0.0.1:50002";
    protected String groupId = "";
    protected String valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    protected String keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
    protected String reqTimeoutMs = "60000";
    //如果缓冲区中没有数据，轮训的间隔时间，单位毫秒
    protected long pollMillions = 200;
    protected int workThreadNum = 4;
    protected LinkedBlockingQueue<ConsumerRecord<String, String>> queue = new LinkedBlockingQueue<ConsumerRecord<String, String>>();
    protected ExecutorService workerExecutor;
    protected ExecutorService monitorExecutor;
    protected KafkaConsumer<String, String> kafkaConsumer;
    protected List<String> topics;
    protected KafkaServerHandler kafkaServerHandler = new KafkaServerHandler();


    /**
     * 启动服务
     */
    @PostConstruct
    public void start() {
        workerExecutor = Executors.newFixedThreadPool(workThreadNum);
        monitorExecutor = Executors.newFixedThreadPool(1);
        Properties props = new Properties();
        //kafka集群地址
        props.setProperty("bootstrap.servers", bootstrapServers);
        // 消费者的groupId
        props.setProperty("group.id", groupId);
        //value的序列化
        props.setProperty("value.deserializer", valueDeserializer);
        //key的序列化
        props.setProperty("key.deserializer", keyDeserializer);
        //请求等待响应时间，单位毫秒。默认值是305000
        props.setProperty("request.timeout.ms", reqTimeoutMs);
        props.setProperty("enable.auto.commit", "true");
        //每次从单个分区中拉取的消息最大尺寸（byte），默认1MB
        props.setProperty("max.partition.fetch.bytes", "1048576");

        kafkaConsumer = new KafkaConsumer<String, String>(props);
        kafkaConsumer.subscribe(topics);
        for (int i = 0; i < workThreadNum; i++) {
            workerExecutor.execute(new WorkerRunnable(queue, kafkaServerHandler));
        }

        monitorExecutor.execute(new MonitorRunnable());

        logger.info("服务连接到Kafka节点" + props.get("bootstrap.servers") + "成功！");

    }


    /**
     * 停止服务
     */
    @PreDestroy
    private void stop() {
        workerExecutor.shutdown();
        monitorExecutor.shutdown();
        kafkaConsumer.close();
        kafkaConsumer = null;
        queue.clear();
        logger.info("Kafka服务停止操作完成.....");
    }
}
