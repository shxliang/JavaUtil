import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author lsx
 */
public class GetDataFromKafka implements Runnable {

    private String topic;
    private String path;

    public GetDataFromKafka(String topic, String path) {
        this.path = path;
        this.topic = topic;
    }

    public static void main(String[] args) {
        GetDataFromKafka gdkast = new GetDataFromKafka("skynet-social-twitter-orientdb-status-v1", "d:\\clusterMonitor.rrd");
        new Thread(gdkast).start();
    }

    @Override
    public void run() {
        System.out.println("start running consumer");

        Properties properties = new Properties();
        properties.put("zookeeper.connect", "90.90.90.5:2181");
        // 必须要使用别的组名称，如果生产者和消费者都在同一组，则不能访问同一组内的topic数据
        properties.put("group.id", "default");
        properties.put("auto.offset.reset", "largest");
        ConsumerConnector consumer = Consumer
                .createJavaConsumerConnector(new ConsumerConfig(properties));

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // 一次从主题中获取一个数据
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> messageStreams = consumer
                .createMessageStreams(topicCountMap);
        // 获取每次接收到的这个数据
        KafkaStream<byte[], byte[]> stream = messageStreams.get(topic).get(0);
        ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
        while (iterator.hasNext()) {
            String message = new String(iterator.next().message());
            // hostName+";"+ip+";"+commandName+";"+res+";"+System.currentTimeMillis();
            // 这里指的注意，如果没有下面这个语句的执行很有可能回从头来读消息的
            consumer.commitOffsets();
            System.out.println(message);
        }
    }
}
