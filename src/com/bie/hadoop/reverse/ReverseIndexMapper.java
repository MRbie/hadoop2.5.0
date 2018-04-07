package com.bie.hadoop.reverse;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class ReverseIndexMapper extends Mapper<Object, Text, Text, Text>{

	private Text word = new Text();
	private Text text = new Text();
	private String filePath;
	
	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		FileSplit fileSplit = (FileSplit) context.getInputSplit();
		filePath = fileSplit.getPath().toString();
	}
	
	@Override
	protected void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		//获取到一行值
		String line = value.toString();
		//分割操作
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			//找到文件路径信息
			text.set(filePath + ":1");
			//打印测试
			System.out.println("key值:" + word + ", value值" + text);
			//输出操作
			context.write(word, text); 
		}
	}
	
	/*@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line);
		while(tokenizer.hasMoreTokens()){
			word.set(tokenizer.nextToken());
			text.set(filePath + ":1");
			context.write(word, text);
		}
	}*/
	
	
}
