package com.bie.hadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

public class HdfsUtils {

	/***
	 * 获取到配置信息
	 * @return
	 */
	public static Configuration getConfiguration(){
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		//重启机器或者设置这个值
		//System.setProperty("hadoop.home.dir","D:/biexiansheng/hadoop/hadoop-2.5.0-cdh5.3.6");
		return conf;
	}
	
	/***
	 * 获取到文件系统
	 * @return
	 */
	public static FileSystem getFileSystem(){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(getConfiguration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
	
	/***
	 * 获取到文件系统
	 * @return
	 */
	public static FileSystem getFileSystem(Configuration conf){
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return fs;
	}
	
}
