package com.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.Properties;

/**
 * kafka发送工具
 * Created by ytt on 2016/4/11.
 */
public class KafkaUtil {

    private static Producer<String, String> producer = null;
    private static Producer<String, String> getProducerInstance(){
        if(producer == null){
            // 设置配置属性
            Properties props = new Properties();
            props.put("metadata.broker.list", KafkaConfig.METADATA_BROKER_LIST);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            // key.serializer.class默认为serializer.class
            props.put("key.serializer.class", "kafka.serializer.StringEncoder");
            // 可选配置，如果不配置，则使用默认的partitioner
            //props.put("partitioner.class", "PartitionerDemo");
            // 触发acknowledgement机制，否则是fire and forget，可能会引起数据丢失
            // 值为0,1,-1,可以参考
            // http://kafka.apache.org/08/configuration.html
            props.put("request.required.acks", KafkaConfig.REQUEST_REQUIRED_ACKS);
            ProducerConfig config = new ProducerConfig(props);

            // 创建producer
            producer = new Producer<>(config);
        }

        return producer;
    }

    public static void send(Long time, String log){
        try {
            KeyedMessage<String, String> data = new KeyedMessage<>(KafkaConfig.TABLE, time.toString(), log);
            getProducerInstance().send(data);
        }catch (Exception ex){
            producer = null;
            System.err.println("send kafka data of log error.ex = " + ex.getMessage());
        }
    }

    public static void sendUser(Long time, String log) {
        try {
            KeyedMessage<String, String> data = new KeyedMessage<>(KafkaConfig.TABLE_USER, time.toString(), log);
            getProducerInstance().send(data);
        }catch (Exception ex){
            producer = null;
            System.err.println("send kafka data of user error.ex = " + ex.getMessage());
        }
    }

    public static void sendBindUser(Long time, String log) {
        try {
            KeyedMessage<String, String> data = new KeyedMessage<>(KafkaConfig.TABLE_BINDUSER, time.toString(), log);
            getProducerInstance().send(data);
        }catch (Exception ex) {
            producer = null;
            System.err.println("send kafka data of bind user error.ex = " + ex.getMessage());
        }
    }
}
