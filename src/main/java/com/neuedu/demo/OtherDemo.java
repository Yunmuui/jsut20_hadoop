package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: OtherDemo
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 10:22
 * @Description: TODO
 */
public class OtherDemo {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);
            Path src=new Path("/mydata/hello.txt");
            Path dst=new Path("/mydata/hello1.txt");
            boolean flag=hdfs.rename(src,dst);
            if(flag){
                System.out.println("文件名修改成功");
            }
            FileStatus fs =hdfs.getFileLinkStatus(new Path("/mydata/hello1.txt"));
            BlockLocation[] blocks =hdfs.getFileBlockLocations(fs,0,fs.getLen());
            for(BlockLocation b : blocks){
                for(int i=0;i<b.getNames().length;i++){
                    System.out.println(b.getNames()[i]+":"+b.getOffset()+":"+b.getLength()+":"+b.getHosts());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
