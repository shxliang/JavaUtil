package util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.*;

/**
 * Created by lsx on 2016/11/10.
 */

public class ArgumentUtil
{
     public static Map<String, String> check(String[] args, List<String> keys){

        // parse parameters
        Map<String, String> map = new HashMap<String, String>();
        for(String arg : args){
            int indexOf = arg.indexOf("=");
            if(indexOf != -1){
                String key = arg.substring(0, indexOf);
                String value = arg.substring(indexOf+1, arg.length());
                map.put(key, value);
            }
        }

        // check if parameters valid
        for(String checkKey : keys) {
            if (!map.containsKey(checkKey)) {
                throw new IllegalArgumentException(checkKey + " must be set.");
            }
        }
        return map;
    }

    public static Map<String, String> check(String[] args){

        // parse parameters
        Map<String, String> map = new HashMap<String, String>();
        for(String arg : args){
            int indexOf = arg.indexOf("=");
            if(indexOf != -1){
                String key = arg.substring(0, indexOf);
                String value = arg.substring(indexOf+1, arg.length());
                map.put(key, value);
            }
        }
        return map;
    }

    public static Map<String, String> load(String[] args, List<String> keys) throws IOException {
        //hdfs | path
        if(args.length == 1 && args[0].startsWith("config=")){
            return load(args[1].substring(args[1].indexOf("=")+1,args[1].length()), keys);
        }else if(args.length == 0){
            return load("local://", keys);
        }else {
            return check(args, keys);
        }
    }

    public static Map<String, String> load(String configPath, List<String> keys) throws IOException {
        Map<String, String> map = new HashMap<>();

        if(configPath.startsWith("hdfs://")){
            Path path = new Path(configPath);
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            InputStream inputStream = fileSystem.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = br.readLine();
            while (null != line){
                int indexOf = line.indexOf("=");
                if(indexOf != -1){
                    String key = line.substring(0, indexOf).trim();
                    String value = line.substring(indexOf+1, line.length()).trim();
                    map.put(key, value);
                }
                line = br.readLine();
            }
        }else if("local://".equals(configPath)){
            ResourceBundle properties = ResourceBundle.getBundle("config");
            Enumeration<String> Keys = properties.getKeys();
            while (Keys.hasMoreElements()){
                String key = Keys.nextElement();
                String value = properties.getString(key);
                map.put(key, new String(value.getBytes("ISO-8859-1"), "UTF-8"));
            }
        }else {
            BufferedReader br =  new BufferedReader(new FileReader(configPath));
            String line = br.readLine();
            while (null != line){
                int indexOf = line.indexOf("=");
                if(indexOf != -1){
                    String key = line.substring(0, indexOf).trim();
                    String value = line.substring(indexOf + 1, line.length()).trim();
                    map.put(key, value);
                }
                line = br.readLine();
            }
        }

        // check if parameters valid
        for(String checkKey : keys) {
            if (!map.containsKey(checkKey)) {
                throw new IllegalArgumentException(checkKey + " must be set.");
            }
        }

        return map;
    }

    public static Map<String, String> load(String[] args) throws IOException {
        if(args.length == 1 && args[0].startsWith("config=")){
            return load(args[1].substring(args[1].indexOf("=")+1,args[1].length()));
        }else if(args.length == 0){
            return load("local://");
        }else {
            return check(args);
        }
    }

    public static Map<String, String> load(String configPath) throws IOException {
        Map<String, String> map = new HashMap<>();

        if(configPath.startsWith("hdfs://")){
            Path path = new Path(configPath);
            FileSystem fileSystem = path.getFileSystem(new Configuration());
            InputStream inputStream = fileSystem.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            String line = br.readLine();
            while (null != line){
                int indexOf = line.indexOf("=");
                if(indexOf != -1){
                    String key = line.substring(0, indexOf).trim();
                    String value = line.substring(indexOf+1, line.length()).trim();
                    map.put(key, value);
                }
                line = br.readLine();
            }
        }else if("local://".equals(configPath)){
            ResourceBundle properties = ResourceBundle.getBundle("config");
            Enumeration<String> Keys = properties.getKeys();
            while (Keys.hasMoreElements()){
                String key = Keys.nextElement();
                String value = properties.getString(key);
                map.put(key, new String(value.getBytes("ISO-8859-1"), "UTF-8"));
            }
        }else {
            BufferedReader br =  new BufferedReader(new FileReader(configPath));
            String line = br.readLine();
            while (null != line){
                int indexOf = line.indexOf("=");
                if(indexOf != -1){
                    String key = line.substring(0, indexOf).trim();
                    String value = line.substring(indexOf + 1, line.length()).trim();
                    map.put(key, value);
                }
                line = br.readLine();
            }
        }
        return map;
    }
}
