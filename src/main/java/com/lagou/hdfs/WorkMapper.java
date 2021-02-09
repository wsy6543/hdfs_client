package com.lagou.hdfs;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class WorkMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {

    IntWritable k = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 读取一行文本并转为IntWritable数据类型，IntWritable类型默认为升序排序
        Integer num = Integer.parseInt(value.toString());
        k.set(num);
        context.write(k, NullWritable.get());
    }
}