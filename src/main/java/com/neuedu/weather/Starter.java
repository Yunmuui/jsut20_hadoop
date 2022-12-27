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
        ALL_PATHS.put("step1_input","/brazil_weather");
        ALL_PATHS.put("step1_output","/step1_output");
        ALL_PATHS.put("step2_input","/step1_output");
        ALL_PATHS.put("step2_output","/step2_output");
        ALL_PATHS.put("step3_input","/step1_output");
        ALL_PATHS.put("step3_output","/step3_output");
        ALL_PATHS.put("step4_input","/step1_output");
        ALL_PATHS.put("step4_output","/step4_output");
    }

    public static void showMenu(){
        System.out.println("--------------------------");
        System.out.println("----- 天气综合查询系统 -----");
        System.out.println("--------------------------");
        System.out.println("1.数据清洗");
        System.out.println("2.查询指定日期的天气");
        System.out.println("3.");
        System.out.println("4.");
        System.out.println("0.退出");
        System.out.print("请输入您的选择（0--9）:");
    }

    public static void manager(){
        boolean exited=false;
        int choice=-1;
        do{
            showMenu();
            choice=scanner.nextInt();
            switch(choice) {
                case 0:
                    break;
                case 1:
                    break;
                case 2:
                    break;
                case 3:
                    break;
                case 4:
            }
        }while(!exited);
        System.out.println("谢谢你的使用，再见！");
    }

    public static void main(String[] args) {
        //Step1.run(ALL_PATHS.get("step1_input"),ALL_PATHS.get("step1_output"));
        //Step2.run(ALL_PATHS.get("step2_input"),ALL_PATHS.get("step2_output"));
        //Step3.run(ALL_PATHS.get("step3_input"),ALL_PATHS.get("step3_output"));
        Step4.run(ALL_PATHS.get("step4_input"),ALL_PATHS.get("step4_output"));
    }
}
