package com.lagou.hdfs;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WorkReduce extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    IntWritable sortIdx = new IntWritable();
    int i = 0;
    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 循环values，确保相同的数字都能输出
        for (NullWritable value : values) {
            sortIdx.set(++i);
            context.write(sortIdx, key);
        }
    }
}