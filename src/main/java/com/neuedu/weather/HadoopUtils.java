package com.neuedu.weather;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: HadoopUtils
 * @Author: Zhangjunnan
 * @Data: 2022/12/26 15:06
 * @Description: TODO
 */
public abstract class HadoopUtils {
    private static Configuration conf;
    static{
        // 实例化配置对象
        conf = new Configuration();
        ///设置hadoop集群对象，若提供xml文件，自动读取，无需设置
        // conf.set("fs.defaultFS","hdfs://master:9000");
    }

    /**
     * 返回HDFS文件系统对象
     *
     * @return HDFS文件系统对象
     */
    public static FileSystem getFileSystem()throws IOException {
        return FileSystem.get(conf);
    }

    public static void delete(FileSystem hdfs, Path dst,boolean isDeleted) throws IOException {
        if(hdfs.exists(dst)){
            hdfs.delete(dst,isDeleted);
        }else{
            System.out.println("文件不存在");
        }
    }

    /**
     * 显示指定目录下所有文件内容
     */
    public static void showContent(FileSystem hdfs,Path dst) throws IOException {
        for(FileStatus fs:hdfs.listStatus(dst)) {
            if(fs.isFile()) {
                FSDataInputStream inputStream = hdfs.open(fs.getPath());
                BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
                String line = reader.readLine();
                while(null!=line){
                    System.out.println(line);
                    line = reader.readLine();
                }
                reader.close();
                inputStream.close();
            }
        }
    }

    /**
     * 返回Configuration对象属性
     *
     * @return Configuration对象属性
     */
    public static Configuration getConf() {
        return conf;
    }
}
