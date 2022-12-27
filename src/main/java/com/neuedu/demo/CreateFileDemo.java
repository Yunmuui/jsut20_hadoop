package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: CreateFileDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 9:34
 * @Description: TODO
 */
public class CreateFileDemo {
    public static void main(String[] args){
        try {
            Configuration conf=new Configuration();
            ///设置hadoop集群对象，若提供xml文件，自动读取，无需设置
            // conf.set("fs.defaultFS","hdfs://master:9000");
            FileSystem hdfs=FileSystem.get(conf);
            byte[] buff="hello world".getBytes();
            Path dst=new Path("/music/hello.txt");
            FSDataOutputStream outputStream=hdfs.create(dst);
            outputStream.write(buff,0,buff.length);
            outputStream.close();
            boolean isExist=hdfs.exists(dst);
            System.out.println(isExist);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
