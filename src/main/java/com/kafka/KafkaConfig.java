package com.kafka;

import java.util.ResourceBundle;

/**
 * kafka配置文件
 * Created by Administrator on 2016/4/11.
 */
public class KafkaConfig {
    public static String TABLE_BINDUSER = "BindUser";
    public static String TABLE_USER = "User";
    public static String METADATA_BROKER_LIST = "108.108.108.15:6667,108.108.108.16:6667,108.108.108.17:6667";
//    public static String METADATA_BROKER_LIST = "222.85.149.5:16667,222.85.149.5:26667,222.85.149.5:36667";
    public static String REQUEST_REQUIRED_ACKS = "1";
    public static String TABLE = "Log";
    public static boolean ISOPEN = true;

    static {
        try {
            ResourceBundle kafka = ResourceBundle.getBundle("kafka-config");
            METADATA_BROKER_LIST = kafka.getString("METADATA_BROKER_LIST");
            REQUEST_REQUIRED_ACKS = kafka.getString("REQUEST_REQUIRED_ACKS");
            TABLE = kafka.getString("TABLE");
            TABLE_USER = kafka.getString("TABLE_USER");
            TABLE_BINDUSER = kafka.getString("TABLE_BINDUSER");
            ISOPEN = Boolean.parseBoolean(kafka.getString("ISOPEN"));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.err.println("KafkaConfig error,ex = " + ex.toString());
        }
    }

}
