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
import java.util.Calendar;
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
            String dayAndMonth=items[0].substring(0,items[0].lastIndexOf("/")).split("_")[1];
            // 获取查询日期
            String date=context.getConfiguration().get("date");
            SimpleDateFormat sdf=new SimpleDateFormat("dd/MM/yyyy");
            Date temp = null;
            try {
                temp = sdf.parse(date);
            } catch (ParseException e) {
                e.printStackTrace();
            }
            // 过滤日期,判断是否跨年
            boolean isInWeek=false;
            String isNewYear="0";
            Calendar cal = Calendar.getInstance();
            cal.setTime(temp);
            int nowYear=cal.get(Calendar.YEAR);
            int i;
            for(i=0;i<7;i++){
                if(dayAndMonth.indexOf(sdf.format(temp).substring(0,sdf.format(temp).lastIndexOf("/")))>=0) {
                    isInWeek=true;
                    // 判断是否跨年
                    if(!sdf.format(temp).contains(String.valueOf(nowYear))){
                        isNewYear="1";
                    }
                    break;
                }
                cal.add(Calendar.DATE,1);
                temp = cal.getTime();
            }
            if(!isInWeek) {
                return;
            }
            //提取日月忽略年
            String day=sdf.format(temp).split("/")[0];
            String month=sdf.format(temp).split("/")[1];
            WeatherWritable w=new WeatherWritable(String.valueOf(i),Double.parseDouble(items[1]),Double.parseDouble(items[2]),Double.parseDouble(items[3]),Double.parseDouble(items[4]));
            context.write(new Text(isNewYear+"/"+month+"/"+day), w);
        }
    }
    private static class StepReducer extends Reducer<Text, WeatherWritable,Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<WeatherWritable> values, Reducer<Text, WeatherWritable, Text, NullWritable>.Context context) throws IOException, InterruptedException {
            String date=context.getConfiguration().get("date");
            String year=date.split("/")[2];
            String month=key.toString().split("/")[1];
            String day=key.toString().split("/")[2];
            if(key.toString().split("/")[0].equals("1")){
                year=String.valueOf(Integer.parseInt(year)+1);
            }
            Double precipitation=0.0,maxTemperature=0.0,minTemperature=0.0,avgTemperature=0.0;
            int count=0;
            for(WeatherWritable v:values){
                precipitation+=v.getPrecipitation();
                maxTemperature+=v.getMaxTemperature();
                minTemperature+=v.getMinTemperature();
                avgTemperature+=v.getAvgTemperature();
                count++;
            }
            String precipitationStr=String.format("%.1f",(precipitation/count));
            String maxTemperatureStr=String.format("%.1f",(maxTemperature/count));
            String minTemperatureStr=String.format("%.1f",(minTemperature/count));
            String avgTemperatureStr=String.format("%.1f",(avgTemperature/count));

            context.write(new Text(year+"/"+month+"/"+day+"\t"+maxTemperatureStr+"°C\t"+minTemperatureStr+"°C\t"+avgTemperatureStr+"°C\t"+precipitationStr+"mm"), NullWritable.get());
        }
    }
    public static void run(String input, String output) {
        try {
            // 输入日期
            Scanner scanner = new Scanner(System.in);
            System.out.println("请输入预测开始日期(yyyy/mm/dd),输入其他信息返回主菜单:");
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
                System.out.println("\t\t\t最高温度\t最低温度\t平均温度\t降雨量");
                HadoopUtils.showContent(HadoopUtils.getFileSystem(),outputPath);
            } else {
                System.out.println("预测失败!");
            }
        } catch (ParseException e){
            return;
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
