package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: TraverseAndOutputTextDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 10:07
 * @Description: TODO
 */
public class TraverseAndOutputTextDemo {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path dst=new Path("/mydata");
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
                    System.out.println("----------------------------------------------------------------");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
