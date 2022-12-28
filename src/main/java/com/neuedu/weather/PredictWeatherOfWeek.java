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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Scanner;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: PredictWeatherOfWeek
 * @Author: Zhangjunnan
 * @Data: 2022/12/28 9:37
 * @Description: TODO
 */
public class PredictWeatherOfWeek {
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
    private static class Step2Reducer extends Reducer<Text, WeatherWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String date=context.getConfiguration().get("date");
            Date temp = null;
            try {
                temp = new SimpleDateFormat("dd/MM/yyyy").parse(date);
                date=new SimpleDateFormat("yyyy/MM/dd").format(temp);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            for(WeatherWritable v:values){
                context.write(new Text(date+"天气数据:\n\t最高温度\t最低温度\t平均温度\t降雨量\n\t"+v.getMaxTemperature()+"°C\t"+v.getMinTemperature()+"°C\t"+v.getAvgTemperature()+"°C\t"+v.getPrecipitation()+"mm"), NullWritable.get());
            }
        }
    }
    public static void run(String input, String output) {
        try {
            // 输入日期
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询开始日期(yyyy/mm/dd),输入其他信息返回主菜单:");
            String date = scanner.next();
            // 转换日期格式
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd");
            // 设置不自动转换错误的日期
            sdf.setLenient(false);
            Date temp = sdf.parse(date);
            date=new SimpleDateFormat("dd/MM/yyyy").format(temp);
            // 传递日期:通过配置对象将参数设置为全局
            HadoopUtils.getConf().set("date",date);
            // 定义输入输出路径
            //String input = "/step1_output";
            //String output = "/step2_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            HadoopUtils.delete(HadoopUtils.getFileSystem(),outputPath,true);
            // 定义job任务
            Job job = Job.getInstance(HadoopUtils.getConf(),"QueryWeatherOfDate");
            // 设置Jar包
            job.setJarByClass(PredictWeatherOfWeek.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(Step2Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(Step2Reducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("查询结束~~!");
                // 显示数据
                HadoopUtils.showContent(HadoopUtils.getFileSystem(),outputPath);
            } else {
                System.out.println("查询失败!");
            }
        } catch (ParseException e){
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
