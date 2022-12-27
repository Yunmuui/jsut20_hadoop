package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: DownLoadDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 9:33
 * @Description: TODO
 */
public class ShowFileDemo {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            FSDataInputStream is = hdfs.open(new Path("/music/hello.txt"));
            int i=is.read();
            while (i!= -1) {
                System.out.print((char)i);
                i=is.read();
            }
            is.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
