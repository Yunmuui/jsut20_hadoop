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
import java.util.HashMap;
import java.util.Scanner;

/**
 * @Projectname: jsut20_hadoop
 * @Filename: QueryMonthWeatherOfYear
 * @Author: Zhangjunnan
 * @Data: 2022/12/28 8:17
 * @Description: TODO
 */
public class QueryMonthWeatherOfYear {
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
            WeatherWritable w=new WeatherWritable(items[0],Double.parseDouble(items[1]),Double.parseDouble(items[2]),Double.parseDouble(items[3]),Double.parseDouble(items[4]));
            //取年份
            String code=items[0].split("_")[0];
            String month=items[0].split("/")[1];
            String year=items[0].split("/")[2];
            if(!year.equals(context.getConfiguration().get("year"))){
                return;
            }
            //输出
            context.write(new Text(code+"_"+year+"_"+month), w);
        }
    }
    private static class StepReducer extends Reducer<Text, WeatherWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
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
            String avgTemperature=String.format("%.1f",(w.getAvgTemperature()/numberDay));
            String year=key.toString().split("_")[1];
            String month=key.toString().split("_")[2];
            HashMap<String, String > monthList  = new HashMap<String, String>(){{
                put("01"," 一月");
                put("02"," 二月");
                put("03"," 三月");
                put("04"," 四月");
                put("05"," 五月");
                put("06"," 六月");
                put("07"," 七月");
                put("08"," 八月");
                put("09"," 九月");
                put("10"," 十月");
                put("11","十一月");
                put("12","十二月");
            }};
            if(month.equals("01")){
                context.write(new Text(year+"年的天气统计:\n\t\t最高气温\t最低气温\t平均气温\t下雨天数\n"+monthList.get(month)+"\t"+w.getMaxTemperature()+"°C\t"+w.getMinTemperature()+"°C\t"+avgTemperature+"°C\t"+rainyDay+"天"),NullWritable.get());
            }else{
                context.write(new Text(monthList.get(month)+"\t"+w.getMaxTemperature()+"°C\t"+w.getMinTemperature()+"°C\t"+avgTemperature+"°C\t"+rainyDay+"天"),NullWritable.get());
            }
        }
    }
    public static void run(String input, String output) {
        try {
            //输入年份
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入查询年份,输入其他信息返回主菜单:");
            String year = scanner.next();
            // 判断输入正确性
            if(Integer.parseInt(year)<1962||Integer.parseInt(year)>2019){
                System.out.println("仅能查询1962-2019年的数据，将返回主菜单");
                return;
            }
            // 传递年份:通过配置对象将参数设置为全局
            HadoopUtils.getConf().set("year",year);
            // 定义输入输出路径
            //String input = "/step1_output";
            //String output = "/step2_output";
            Path outputPath = new Path(output);
            // 判断输出目录是否存在，存在则删除之
            HadoopUtils.delete(HadoopUtils.getFileSystem(),outputPath,true);
            // 定义job任务
            Job job = Job.getInstance(HadoopUtils.getConf(),"QueryMonthWeatherOfYear");
            // 设置Jar包
            job.setJarByClass(QueryMonthWeatherOfYear.class);
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
                System.out.println("统计结束~~!");
                // 显示数据
                HadoopUtils.showContent(HadoopUtils.getFileSystem(),outputPath);
            } else {
                System.out.println("统计失败!");
            }
        }catch (NumberFormatException e){
            return;
        }  catch (Exception e) {
            e.printStackTrace();
        }
    }
}
