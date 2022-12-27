package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: RemoveFileAndDirectoryDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 10:18
 * @Description: TODO
 */
public class RemoveFileAndDirectoryDemo {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path dst = new Path("/music/hello.txt");
            boolean isdeleted = hdfs.delete(dst,false);
            System.out.println(isdeleted);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
