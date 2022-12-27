package com.neuedu.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.Scanner;

/**
 * Step2查询某1天的天气数据
 * 传递查询的日期
 * @Projectname: jsut20_hadoop
 * @Filename: Step2
 * @Author: Zhangjunnan
 * @Data: 2022/12/27 8:21
 * @Description: TODO
 */
public class Step2 {
    private static class Step2Mapper extends Mapper<LongWritable, Text,Text,WeatherWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            String line=value.toString();
            // 数据验证
            if(StringUtils.isBlank(line)){
                return;
            }
            // 数据拆分
            String[] items=line.split("\t",5);
            if(items==null||items.length!=5){
                return;
            }
            // 获取查询日期
            String date=context.getConfiguration().get("date");
            // 过滤日期
            if(items[0].indexOf(date)<0) {
                return;
            }
            WeatherWritable w=new WeatherWritable(items[0],Double.parseDouble(items[1]),Double.parseDouble(items[2]),Double.parseDouble(items[3]),Double.parseDouble(items[4]));
            context.write(new Text(items[0]), w);
        }
    }
    private static class Step2Reducer extends Reducer<Text, WeatherWritable,WeatherWritable,NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            for(WeatherWritable v:values){
                context.write(v, NullWritable.get());
            }
        }
    }
    public static void run(String input, String output) {
        try {
            //输入日期
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询日期(dd/mm/yyyy):");
            String date = scanner.next();
            // 传递日期:通过配置对象将参数设置为全局
            HadoopUtils.getConf().set("date",date);
            // 定义输入输出路径
            //String input = "/step1_output";
            //String output = "/step2_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            HadoopUtils.delete(HadoopUtils.getFileSystem(),outputPath,true);
            // 定义job任务
            Job job = Job.getInstance(HadoopUtils.getConf(),"step2");
            // 设置Jar包
            job.setJarByClass(Step2.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(Step2.Step2Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(Step2.Step2Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("Step2:查询结束~~!");
                // 显示数据
                HadoopUtils.showContent(HadoopUtils.getFileSystem(),outputPath);
            } else {
                System.out.println("Failure!");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
