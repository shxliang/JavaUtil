package kafka;

import java.util.ResourceBundle;

/**
 * kafka配置文件
 *
 * @author Administrator
 * @date 2016/4/11
 */
public class KafkaConfig {
    public static String TABLE_BINDUSER = "BindUser";
    public static String TABLE_USER = "User";
    public static String METADATA_BROKER_LIST;
    public static String REQUEST_REQUIRED_ACKS = "1";
    public static String TABLE;
    public static boolean ISOPEN = true;

    static {
        try {
            ResourceBundle kafka = ResourceBundle.getBundle("kafka-config");
            METADATA_BROKER_LIST = kafka.getString("METADATA_BROKER_LIST");
            REQUEST_REQUIRED_ACKS = kafka.getString("REQUEST_REQUIRED_ACKS");
            TABLE = kafka.getString("TABLE");
            ISOPEN = Boolean.parseBoolean(kafka.getString("ISOPEN"));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.err.println("KafkaConfig error,ex = " + ex.toString());
        }
    }
}
