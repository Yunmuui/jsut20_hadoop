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

/**
 * Step1数据清洗
 *
 * @Projectname: jsut20_hadoop
 * @Filename: Step1
 * @Author: Zhangjunnan
 * @Data: 2022/12/26 16:04
 * @Description: TODO
 */
public class Step1 {
    private static class Step1Mapper extends Mapper<LongWritable,Text, Text,WeatherWritable> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, WeatherWritable>.Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if(StringUtils.isBlank(line)){
                return;
            }
            //跳过标题栏
            if(line.startsWith("Estacao")){
                return;
            }
            // 数据量太多，挑选巴西利亚数据，站点编号83377
            if(!line.startsWith("83377")){
                return;
            }
            // 拆分数据
            String[] items=line.split(";",19);
            if(items.length!=19){
                return;
            }
            // 提取数据
            String code=items[0];
            String date=items[1];
            String precipitation=StringUtils.isBlank(items[3])?"0":items[3];
            String max=StringUtils.isBlank(items[6])?"0":items[6];
            String min=StringUtils.isBlank(items[7])?"0":items[7];
            String avg=StringUtils.isBlank(items[16])?"0":items[16];
            // 实例对象
            WeatherWritable w = new WeatherWritable(code+"_"+date,
                    Double.valueOf(precipitation),Double.valueOf(max),
                    Double.valueOf(min),Double.valueOf(avg));
            // 输出
            context.write(new Text(w.getCode_date()),w);
        }
    }
    private static class Step1Reducer extends Reducer<Text,WeatherWritable,WeatherWritable, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, WeatherWritable, NullWritable>.Context context) throws IOException, InterruptedException {
            // 实例化对象
            WeatherWritable w=new WeatherWritable(key.toString(),0.0,0.0,0.0,0.0);
            // 三行合并
            for(WeatherWritable v:values){
                // 三行累加
                w.setPrecipitation(w.getPrecipitation() + v.getPrecipitation());
                w.setMaxTemperature(w.getMaxTemperature()+v.getMaxTemperature());
                w.setMinTemperature(w.getMinTemperature()+v.getMinTemperature());
                w.setAvgTemperature(w.getAvgTemperature()+v.getAvgTemperature());
            }
            // 数据验证
            if(w.getMaxTemperature()<=0||w.getMinTemperature()<=0||w.getAvgTemperature()<=0){
                return;
            }
            // 输出
            context.write(w,NullWritable.get());
        }
    }
    public static void run(String input, String output) {
        try {
            // 定义输入输出路径
            //String input = "/brazil_weather";
            //String output = "/step1_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            HadoopUtils.delete(HadoopUtils.getFileSystem(),outputPath,true);
            // 定义job任务
            Job job = Job.getInstance(HadoopUtils.getConf(),"step1");
            // 设置Jar包
            job.setJarByClass(Step1.class);
            // 设定输入
            job.setInputFormatClass(TextInputFormat.class);
            FileInputFormat.setInputPaths(job,input);
            // 设置自定义Mapper
            job.setMapperClass(Step1Mapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(WeatherWritable.class);
            // 设置自定义Reducer
            job.setReducerClass(Step1Reducer.class);
            job.setOutputKeyClass(WeatherWritable.class);
            job.setOutputValueClass(NullWritable.class);
            // 设置输出文件
            job.setOutputFormatClass(TextOutputFormat.class);
            FileOutputFormat.setOutputPath(job,outputPath);
            // 运行
            boolean success = job.waitForCompletion(true);
            if(success) {
                System.out.println("Step1:数据清洗结束~~!");
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
