package com.bie.hadoop.reverse;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.jcraft.jsch.Buffer;

public class ReverseIndexReducer extends Reducer<Text, Text, Text, Text>{

	private Text outputValue = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		StringBuffer sb = new StringBuffer();
		Map<String,Integer> map = new HashMap<String,Integer>();
		//
		for(Text value : values){
			//新建一个对象sb
			sb = new StringBuffer();
			//先将信息进行删除操作
			//sb.delete(0, sb.length() - 1);
			//获取到一行的信息
			String line = value.toString();
			//将这一行信息进行反转
			line = sb.append(line).reverse().toString();
			//将反转的信息进行分割操作
			String[] strs = line.split(":",2);
			//获取到反转分割得到的文件路径信息,将新的信息添加进来
			String path = sb.delete(0, sb.length() - 1).append(strs[1]).reverse().toString();
			//获取到值
			Integer count = Integer.valueOf(strs[0]);
			//如果包含这个就叠加，不包含新建
			if(map.containsKey(path)){
				map.put(path, map.get(path) + count);
			}else{
				map.put(path, count);
			}
		}
		sb.delete(0, sb.length() - 1);
		//对权重信息进行处理操作
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			sb.append(entry.getKey()).append(":")
				.append(entry.getValue()).append(";");
		}
		outputValue.set(sb.deleteCharAt(sb.length() - 1).toString());
		context.write(key, outputValue);
	}
	
	
}
