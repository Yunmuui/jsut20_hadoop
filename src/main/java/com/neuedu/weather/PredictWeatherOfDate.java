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
 * 预测指定日期天气
 * @Projectname: jsut20_hadoop
 * @Filename: Step4
 * @Author: Zhangjunnan
 * @Data: 2022/12/27 14:21
 * @Description: TODO
 */
public class PredictWeatherOfDate {
    private static class StepMapper extends Mapper<LongWritable, Text,Text,WeatherWritable> {
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
            // 历史上的今天，不关注年份
            String day_month=date.substring(0,date.lastIndexOf("/"));
            // 过滤日期
            if(items[0].indexOf(day_month)<0) {
                return;
            }
            String code=items[0].split("_")[0];

            WeatherWritable w=new WeatherWritable(code+"_"+day_month,Double.parseDouble(items[1]),Double.parseDouble(items[2]),Double.parseDouble(items[3]),Double.parseDouble(items[4]));
            context.write(new Text(w.getCode_date()), w);
        }
    }
    private static class StepReducer extends Reducer<Text, WeatherWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            WeatherWritable w = new WeatherWritable(key.toString(),0.0,0.0,0.0,0.0);
            int count=0;
            for(WeatherWritable v:values){
                w.setPrecipitation(w.getPrecipitation()+v.getPrecipitation());
                w.setMaxTemperature(w.getMaxTemperature()+v.getMaxTemperature());
                w.setMinTemperature(w.getMinTemperature()+v.getMinTemperature());
                w.setAvgTemperature(w.getAvgTemperature()+v.getAvgTemperature());
                count++;
            }
            String precipitation=String.format("%.2f",(w.getPrecipitation()/count));
            String maxTemperature=String.format("%.1f",(w.getMaxTemperature()/count));
            String minTemperature=String.format("%.1f",(w.getMinTemperature()/count));
            String avgTemperature=String.format("%.1f",(w.getAvgTemperature()/count));
            String date=context.getConfiguration().get("date");
            Date temp = null;
            try {
                temp = new SimpleDateFormat("dd/MM/yyyy").parse(date);
                date=new SimpleDateFormat("yyyy/MM/dd").format(temp);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            context.write(new Text(date+"预测数据:\n\t最高温度\t最低温度\t平均温度\t降雨量\n\t"+maxTemperature+"°C\t"+minTemperature+"°C\t"+avgTemperature+"°C\t"+precipitation+"mm"),NullWritable.get());
        }
    }
    public static void run(String input, String output) {
        try {
            //输入日期
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询日期(yyyy/mm/dd),输入其他信息返回主菜单:");
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
            Job job = Job.getInstance(HadoopUtils.getConf(),"step4");
            // 设置Jar包
            job.setJarByClass(PredictWeatherOfDate.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(StepMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(StepReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("预测结束~~!");
                // 显示数据
                HadoopUtils.showContent(HadoopUtils.getFileSystem(),outputPath);
            } else {
                System.out.println("预测失败!");
            }
        } catch (ParseException e){
            return;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
