package com.neuedu.weather;

import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * 程序主控类，调用各个业务算法，实现系统所有功能
 * @Projectname: jsut20_hadoop
 * @Filename: Starter
 * @Author: Zhangjunnan
 * @Data: 2022/12/26 15:15
 * @Description: TODO
 */
public class Starter {
    private static Map<String, String> ALL_PATHS = new HashMap<String,String>();

    private static Scanner scanner = new Scanner(System.in);

    static {
        ALL_PATHS.put("dataCleaning_input","/brazil_weather");
        ALL_PATHS.put("dataCleaning_output","/dataCleaning_output");
        ALL_PATHS.put("queryWeatherOfDate_input","/dataCleaning_output");
        ALL_PATHS.put("queryWeatherOfDate_output","/queryWeatherOfDate_output");
        ALL_PATHS.put("queryWeatherOfYear_input","/dataCleaning_output");
        ALL_PATHS.put("queryWeatherOfYear_output","/queryWeatherOfYear_output");
        ALL_PATHS.put("queryMonthWeatherOfYear_input","/dataCleaning_output");
        ALL_PATHS.put("queryMonthWeatherOfYear_output","/queryWeather_output");
        ALL_PATHS.put("predictWeatherOfDate_input","/dataCleaning_output");
        ALL_PATHS.put("predictWeatherOfDate_output","/predictWeatherOfDate_output");
        ALL_PATHS.put("predictWeatherOfWeek_input","/dataCleaning_output");
        ALL_PATHS.put("predictWeatherOfWeek_output","/predictWeatherOfWeek_output");
    }

    public static void showMenu(){
        System.out.println("--------------------------");
        System.out.println("----- 天气综合查询系统 -----");
        System.out.println("--------------------------");
        System.out.println("1.查询指定日期的天气");
        System.out.println("2.查询指定年份的天气统计");
        System.out.println("3.查询指定年份每月天气");
        System.out.println("4.预测一日天气");
        System.out.println("5.预测一周天气");
        System.out.println("0.退出");
        System.out.print("请输入您的选择（0--5）:");
    }

    public static void manager(){
        boolean exited=false;
        int choice=-1;
        do{
            showMenu();
            choice=scanner.nextInt();
            switch(choice) {
                case 0:
                    exited=true;
                    break;
                case 1:
                    QueryWeatherOfDate.run(ALL_PATHS.get("queryWeatherOfDate_input"),ALL_PATHS.get("queryWeatherOfDate_output"));
                    break;
                case 2:
                    QueryWeatherOfYear.run(ALL_PATHS.get("queryWeatherOfYear_input"),ALL_PATHS.get("queryWeatherOfYear_output"));
                    break;
                case 3:
                    QueryMonthWeatherOfYear.run(ALL_PATHS.get("queryMonthWeatherOfYear_input"),ALL_PATHS.get("queryMonthWeatherOfYear_output"));
                    break;
                case 4:
                    PredictWeatherOfDate.run(ALL_PATHS.get("predictWeatherOfDate_input"),ALL_PATHS.get("predictWeatherOfDate_output"));
                    break;
                case 5:
                    PredictWeatherOfWeek.run(ALL_PATHS.get("predictWeatherOfWeek_input"),ALL_PATHS.get("predictWeatherOfWeek_output"));
                    break;
                default:
                    System.out.println("输入错误！请输入数字（0--5）!");
                    break;
            }
            System.out.println("***按回车键继续***");
            try {System.in.read();} catch(Exception e) {}
        }while(!exited);
        System.out.println("谢谢你的使用，再见！");
    }

    public static void main(String[] args) {
        /// 清洗数据，测试不用
        //CleanData.run(ALL_PATHS.get("dataCleaning_input"),ALL_PATHS.get("dataCleaning_output"));
        manager();
    }
}
