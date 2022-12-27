package com.neuedu.weather;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Hadoop实体类WeatherWritable
 * 存储和传输数据
 * map输出后自动排序
 *
 * @Projectname: jsut20_hadoop
 * @Filename: WeatherWritable
 * @Author: Zhangjunnan
 * @Data: 2022/12/26 15:25
 * @Description: TODO
 */
public class WeatherWritable implements WritableComparable<WeatherWritable> {
    private String code_date;
    private Double precipitation;
    private Double maxTemperature;
    private Double minTemperature;
    private Double avgTemperature;

    public WeatherWritable() {
    }

    public WeatherWritable(String code_date, Double precipitation, Double maxTemperature, Double minTemperature, Double avgTemperature) {
        this.code_date = code_date;
        this.precipitation = precipitation;
        this.maxTemperature = maxTemperature;
        this.minTemperature = minTemperature;
        this.avgTemperature = avgTemperature;
    }

    @Override
    public int compareTo(WeatherWritable other) {
        if(other==null){
            return 1;
        }
        return this.code_date.compareTo(other.code_date);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        // hadoop序列化
        out.writeUTF(this.code_date);
        out.writeDouble(this.precipitation);
        out.writeDouble(this.maxTemperature);
        out.writeDouble(this.minTemperature);
        out.writeDouble(this.avgTemperature);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // hadoop反序列化
        this.code_date = in.readUTF();
        this.precipitation = in.readDouble();
        this.maxTemperature = in.readDouble();
        this.minTemperature = in.readDouble();
        this.avgTemperature = in.readDouble();
    }

    @Override
    public String toString() {
        return code_date + "\t" +
               precipitation + "\t" + maxTemperature + "\t" +
               minTemperature + "\t" + avgTemperature;
    }

    public String getCode_date() {
        return code_date;
    }

    public void setCode_date(String code_date) {
        this.code_date = code_date;
    }

    public Double getPrecipitation() {
        return precipitation;
    }

    public void setPrecipitation(Double precipitation) {
        this.precipitation = precipitation;
    }

    public Double getMaxTemperature() {
        return maxTemperature;
    }

    public void setMaxTemperature(Double maxTemperature) {
        this.maxTemperature = maxTemperature;
    }

    public Double getMinTemperature() {
        return minTemperature;
    }

    public void setMinTemperature(Double minTemperature) {
        this.minTemperature = minTemperature;
    }

    public Double getAvgTemperature() {
        return avgTemperature;
    }

    public void setAvgTemperature(Double avgTemperature) {
        this.avgTemperature = avgTemperature;
    }
}
