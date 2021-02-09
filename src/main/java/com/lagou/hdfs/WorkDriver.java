package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class WorkDriver extends Configured implements Tool {

    /**
     * 运行入口
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        int status = ToolRunner.run(new WorkDriver(), args);
        System.exit(status);
    }

    @Override
    public int run(String[] args) throws Exception {
        //1 获取配置信息
        Configuration conf = getConf();
        //2 获取Job实例对象并设置任务名称
        Job job = Job.getInstance(conf, "Homework");
        //3 指定运行的jar
        job.setJarByClass(WorkDriver.class);
        //4 设置Mapper/Reducer的业务类
        job.setMapperClass(WorkMapper.class);
        job.setReducerClass(WorkReduce.class);
        //5 设置Mapper输出的数据kv类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        //6 设置最终输出的数据kv类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        //7 设置ReduceTask个数，默认为1
        job.setNumReduceTasks(1);
        //8 设置Job的输入输出的路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        //9 提交运行
        boolean status = job.waitForCompletion(true);
        return status ? 0 : 1;
    }
}