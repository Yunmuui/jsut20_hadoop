package com.neuedu.weather;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
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
import java.util.Scanner;

/**
 * 查询某年最高气温、最低气温、平均气温、下雨天数
 * @Projectname: jsut20_hadoop
 * @Filename: Step3
 * @Author: Zhangjunnan
 * @Data: 2022/12/27 9:15
 * @Description: TODO
 */
public class Step3 {
    private static class Step3Mapper extends Mapper<LongWritable, Text,Text,WeatherWritable> {
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
            WeatherWritable w=new WeatherWritable(items[0],Double.parseDouble(items[1]),Double.parseDouble(items[2]),Double.parseDouble(items[3]),Double.parseDouble(items[4]));
            //取年份
            String code=items[0].split("_")[0];
            String year=items[0].substring(items[0].lastIndexOf("/"));
            //输出
            context.write(new Text(code+"_"+year), w);
        }
    }
    private static class Step3Reducer extends Reducer<Text, WeatherWritable,Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, Text>.Context context) throws IOException, InterruptedException {
            WeatherWritable w = new WeatherWritable();
            w.setCode_date(key.toString());
            w.setMaxTemperature(Double.MIN_VALUE);
            w.setMinTemperature(Double.MAX_VALUE);
            w.setAvgTemperature(0.0);
            int rainyDay=0;
            int numberDay=0;
            for(WeatherWritable v:values){
                if(v.getMaxTemperature()>w.getMaxTemperature()){
                    w.setMaxTemperature(v.getMaxTemperature());
                }
                if(v.getMinTemperature()<w.getMinTemperature()){
                    w.setMinTemperature(v.getMinTemperature());
                }
                w.setAvgTemperature(w.getAvgTemperature()+v.getAvgTemperature());
                if(v.getPrecipitation()>0){
                    rainyDay+=1;
                }
                numberDay+=1;
            }
            String avgTemperature=String.format("%.1f",w.getAvgTemperature()/numberDay);
            context.write(new Text(key), new Text(""+w.getMaxTemperature()+"\t"+w.getMinTemperature()+"\t"+avgTemperature+"\t"+rainyDay));
        }
    }
    public static void run(String input, String output) {
        try {
            //输入年份
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询年份:");
            String year = scanner.next();
            // 传递年份:通过配置对象将参数设置为全局
            HadoopUtils.getConf().set("year",year);
            // 定义输入输出路径
            //String input = "/step1_output";
            //String output = "/step2_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            HadoopUtils.delete(HadoopUtils.getFileSystem(),outputPath,true);
            // 定义job任务
            Job job = Job.getInstance(HadoopUtils.getConf(),"step3");
            // 设置Jar包
            job.setJarByClass(Step3.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(Step3Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(Step3Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("Step3:统计结束~~!");
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
