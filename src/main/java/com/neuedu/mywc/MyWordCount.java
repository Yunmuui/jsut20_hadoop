package com.neuedu.mywc;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

/**
 * 词频统计：本地运行版本
 * @Projectname: jsut20_hadoop
 * @Filename: MyWordCount
 * @Author: Zhangjunnan
 * @Data: 2022/12/26 9:14
 * @Description: TODO
 */
public class MyWordCount {
    /**
     * 自定义Mapper自定义处理类：负责读取所有内容，
     */
    private static class WordCountMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //数据清洗与数据验证
            String line=value.toString().toString();
            if(StringUtils.isEmpty(line)){
                return;
            }
            //单词拆分
            StringTokenizer st = new StringTokenizer(line);
            while (st.hasMoreTokens()) {
                // 提取单词
                String word = st.nextToken();
                context.write(new Text(word), new IntWritable(1));
            }
        }
    }
    private static class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //统计单词的总次数
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            //输出
            context.write(key,new IntWritable(sum));
        }
    }
    public static void main(String[] args) {
        try {
            Configuration conf=new Configuration();
            ///设置hadoop集群对象，若提供xml文件，自动读取，无需设置
            // conf.set("fs.defaultFS","hdfs://master:9000");
            FileSystem hdfs=FileSystem.get(conf);
            // 定义输入输出路径
            String input = "/books";
            String output = "/wc_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            if(hdfs.exists(outputPath)){
                hdfs.delete(outputPath,true);
            }
            // 定义job任务
            Job job = Job.getInstance(conf,"word count");
            // 设置Jar包
            job.setJarByClass(MyWordCount.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(WordCountMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(WordCountReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("词频统计结束!");
            } else {
                System.out.println("Failure!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
