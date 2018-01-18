package redis;

import java.lang.reflect.Field;
import java.util.ResourceBundle;

/**
 *
 * @author k
 * @date 8/15/17
 */
public class Config {

    public static void init(ResourceBundle bundle, Class clazz) {
        for (Field field : clazz.getDeclaredFields()) {
            try {
                String key = field.getName().replaceAll("_", ".").toLowerCase();
                if (bundle.containsKey(key)) {
                    Class<?> type = field.getType();
                    if ("int".equals(type.getName())) {
                        field.setInt(null, Integer.parseInt(bundle.getString(key)));
                    } else if ("boolean".equals(type.getName())) {
                        field.setBoolean(null, Boolean.parseBoolean(bundle.getString(key)));
                    } else {
                        field.set(null, bundle.getObject(key));
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }

}
