package conf;

import java.util.ResourceBundle;

/**
 * kafka配置文件
 * Created by tt on 2016/4/20.
 */
public class KafkaConfig extends Config {

    public static String METADATA_BROKER_LIST;
    public static String AUTO_OFFSET_RESET;
    public static String TOPICS;

    static {
        ResourceBundle kafka = ResourceBundle.getBundle("kafka");
        init(kafka, KafkaConfig.class);
    }
}
