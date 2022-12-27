package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: ListDirectoryDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 10:00
 * @Description: TODO
 */
public class ListDirectoryDemo {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            FileStatus[] status = hdfs.listStatus(new Path("/music"));
            for (FileStatus sta : status) {
                System.out.println(sta);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
