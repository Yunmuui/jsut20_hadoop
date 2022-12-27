package com.neuedu.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: Demo1
 * @Author: Zhangjunnan
 * @Data: 2022/12/23 8:22
 * @Description: TODO
 */
public class UpLoadDemo {
    public static void main(String[] args) {
        try {
            //实例化配置对象
            Configuration conf = new Configuration();
            ///设置hadoop集群对象，若提供xml文件，自动读取，无需设置
            // conf.set("fs.defaultFS","hdfs://master:9000");
            // 获取HDFS对象
            FileSystem hdfs = FileSystem.get(conf);
            //定义源路径
            Path src=new Path("C:/Users/Yun_mu/Desktop/school/实训/f.txt");
            // 定义目标路径
            Path dst=new Path("/mydata/f.txt");
            //文件上传
            hdfs.copyFromLocalFile(src, dst);
            // 判断目标文件是否存在
            if (hdfs.exists(dst)) {
                System.out.println("文件上传成功");
            }
        }catch(Exception e){
            e.printStackTrace();
        }
    }
}
