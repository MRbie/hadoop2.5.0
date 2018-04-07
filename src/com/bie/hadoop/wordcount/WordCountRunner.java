package com.bie.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bie.hadoop.utils.HdfsUtils;

public class WordCountRunner {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//创建配置文件
        //Configuration conf = new Configuration();
        Configuration conf = HdfsUtils.getConfiguration();
		
		//获取一个作业
        Job job = Job.getInstance(conf);
        
        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(WordCountRunner.class);
        
        //本job使用的mapper和reducer的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        
        //指定reduce的输出数据key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);
        
        
        //指定mapper的输出数据key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        //指定要处理的输入数据存放路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://master:9000/wc/srcdata"));
        
        //指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://master:9000/wc/output"));
        
        //将job提交给集群运行 
        job.waitForCompletion(true);
	}
	
}
