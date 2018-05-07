package com.bie.hadoop.client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HadoopClient {

	private Configuration conf = null;
	private FileSystem fs = null;
	
	@Before
	public void init(){
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://slaver1:9000");
		try {
			fs = FileSystem.get(conf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	//上传文件
	@Test
	public void uploadFile(){
		try {
			fs.copyFromLocalFile(new Path("E://20180507.txt"), new Path("/201805/20180507.txt"));
			System.out.println("文件上传结束......");
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Test
	public void downloadFile(){
		try {
			fs.copyToLocalFile(new Path("/201805/20180507"), new Path("E:/"));
			System.out.println("文件下载结束......");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void deleteFile(){
		try {
			fs.deleteOnExit(new Path("/201805/20180507"));
			System.out.println("文件删除结束......");
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void mkdirFiles(){
		try {
			fs.mkdirs(new Path("/201805/20180507"));
			System.out.println("创建目录结束......");
		} catch (IllegalArgumentException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void testConf(){
		Iterator<Entry<String, String>> it = conf.iterator();
		while(it.hasNext()){
			Entry<String, String> next = it.next();
			System.out.println("key:" + next.getKey() + ",value:" + next.getValue());
		}
		System.out.println("查看conf结束......");
	}
	
	@After
	public void close(){
		try {
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	public static void main(String[] args) {
		
		
	}
	
}
