package redis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;
import java.util.Set;

/**
 * db2存储IP、UV缓存等信息
 * @author lsx
 */
public class RedisClient {
    /**
     * 初始化连接池
     */
    private static JedisPool pool = new JedisPool(new GenericObjectPoolConfig(), RedisConfig.HOST, 6379, 10000, "abcabc");

    public static Jedis getJedis() {
        Jedis jedis = pool.getResource();
        jedis.select(RedisConfig.DB);
        return jedis;
    }

    /**
     * 给指定key的set表中添加数据
     * @param key
     * @param value
     */
    public static void set(String key, String value) {
        Jedis jedis = getJedis();
        jedis.set(key, value);
        jedis.close();
    }

    public static String get(String key) {
        Jedis jedis = getJedis();
        String value = jedis.get(key);
        jedis.close();
        return value;
    }

    public static void incrBy(String key, Long value)
    {
        Jedis jedis = getJedis();
        jedis.incrBy(key,value);
        jedis.close();
    }

    /**
     * 自增
     * @param key
     * @param field
     * @param value
     */
    public static void hincrBy(String key, String field, Long value) {
        Jedis jedis = getJedis();
        jedis.hincrBy(key, field, value);
        jedis.close();
    }

    public static void del(String name){
        Jedis jedis = getJedis();
        jedis.del(name);
        jedis.close();
    }

    /**
     * 移除指定key对应的value值的String对象
     * @param key
     * @param condition
     * @param value
     */
    public static void lrem(String key,int condition,String value){
        Jedis jedis = getJedis();
        jedis.lrem(key, condition,value);
        jedis.close();
    }

    /**
     * 返回指定key的Set数组
     * @param key
     * @return
     */
    public static Set<String> smembers(String key){
        Jedis jedis = getJedis();
        Set<String> keySet = jedis.smembers(key);
        jedis.close();
        return keySet;
    }

    /**
     * 给指定key的Set添加元素
     * @param key
     * @param value
     */
    public static void sadd(String key ,String value){
        Jedis jedis = getJedis();
        jedis.sadd(key, value);
        jedis.close();
    }

    /**
     * 给指定key的list添加元素
     * @param key
     * @param value
     */
    public static void lpush(String key,String value){
        Jedis jedis = getJedis();
        jedis.lpush(key,value);
        jedis.close();
    }

    /**
     * 指定key 返回list列表
     * @param key
     * @return
     */
    public static List<String> lrange(String key){
        Jedis jedis = getJedis();
        List<String> lrange = jedis.lrange(key, 0, -1);
        jedis.close();
        return lrange;
    }

    public static boolean exists(String key)
    {
        Jedis jedis = getJedis();
        boolean exist = jedis.exists(key);
        jedis.close();
        return exist;
    }

    public static Set<String> keys(String pattern)
    {
        Jedis jedis = getJedis();
        Set<String> keys = jedis.keys(pattern);
        jedis.close();
        return keys;
    }

    public static void hset(String key, String field, String value)
    {
        Jedis jedis = getJedis();
        jedis.hset(key, field, value);
        jedis.close();
    }

    public static void hincrBy(String key, String field, long value)
    {
        Jedis jedis = getJedis();
        jedis.hincrBy(key, field, value);
        jedis.close();
    }

    public static void expire(String key, int seconds)
    {
        Jedis jedis = getJedis();
        jedis.expire(key, seconds);
        jedis.close();
    }

    public static void rpush(String key,String value){
        Jedis jedis = getJedis();
        jedis.rpush(key,value);
        jedis.close();
    }

    /**
     * 给指定key的Set添加多个元素
     * @param key
     * @param values
     */
    public static void sadd(String key ,String[] values){
        Jedis jedis = getJedis();
        jedis.sadd(key, values);
        jedis.close();
    }
}