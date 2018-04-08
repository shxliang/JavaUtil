package conf;

import java.util.ResourceBundle;

/**
 * Created by Administrator on 2016/5/4.
 */
public class MySQLConfig extends Config {

    public static String HOST;
    public static int DB = 1;

    static {
        ResourceBundle redis = ResourceBundle.getBundle("mysql");
        init(redis, MySQLConfig.class);
    }

}
