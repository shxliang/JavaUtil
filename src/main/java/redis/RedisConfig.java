package redis;

import java.util.ResourceBundle;

public class RedisConfig extends Config {

    public static String HOST;
    public static int DB;

    static {
        ResourceBundle redis = ResourceBundle.getBundle("redis");
        Config.init(redis, RedisConfig.class);
    }
}
