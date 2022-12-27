package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: ShowFileDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 9:54
 * @Description: TODO
 */
public class ShowFileDemo1 {
    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            ///设置hadoop集群对象，若提供xml文件，自动读取，无需设置
            // conf.set("fs.defaultFS","hdfs://master:9000");
            FileSystem hdfs=FileSystem.get(conf);
            Path dst = new Path("/mydata/f.txt");
            FSDataInputStream inputStream=hdfs.open(dst);
            BufferedReader reader=new BufferedReader(new InputStreamReader(inputStream));
            String line = reader.readLine();
            while(null!=line){
                System.out.print(line);
                line = reader.readLine();
            }
            reader.close();
            inputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
