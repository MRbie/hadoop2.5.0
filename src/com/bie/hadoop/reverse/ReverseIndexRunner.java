package com.bie.hadoop.reverse;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bie.hadoop.utils.HdfsUtils;

public class ReverseIndexRunner{

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//创建配置文件
        //Configuration conf = new Configuration();
        Configuration conf = HdfsUtils.getConfiguration();
		
		//获取一个作业
        Job job = Job.getInstance(conf, "reverseIndex");
        
        //设置整个job所用的那些类在哪个jar包
        job.setJarByClass(ReverseIndexRunner.class);
        
        //本job使用的mapper和reducer的类
        job.setMapperClass(ReverseIndexMapper.class);
        job.setReducerClass(ReverseIndexReducer.class);
        
        //指定mapper的输出数据key-value类型
        //job.setMapOutputKeyClass(LongWritable.class);
        //job.setMapOutputValueClass(Text.class);
        
        //指定reduce的输出数据key-value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        
        //指定要处理的输入数据存放路径
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.19.131:9000/reverse/input"));
        
        //指定处理结果的输出数据存放路径
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.19.131:9000/reverse/output"));
        
        //将job提交给集群运行 
        job.waitForCompletion(true);
	}

	
}
