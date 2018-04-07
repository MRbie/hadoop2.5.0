package com.bie.hadoop.wordcount;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable>{

	//创建全部对象,避免多次创建浪费资源
	private Text word = new Text();
	private LongWritable one = new LongWritable(1);
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("调用了WordCountMapper的setup方法");
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//将value即一行,转换为字符串
		String line = value.toString();
		//对字符串进行分割
		StringTokenizer tokenizer = new StringTokenizer(line);
		//是否有下一个
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			//设置值,一个设置一次1
			context.write(word, one);
		}
	}
	
	@Override
	protected void cleanup(Mapper<LongWritable, Text, Text, LongWritable>.Context context)
			throws IOException, InterruptedException {
		System.out.println("调用了WordCountMapper的cleanup方法");
	}
}
