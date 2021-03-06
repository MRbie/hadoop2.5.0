package com.bie.hadoop.client;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
	
	@Test
	public void listFile(){
		try {
			RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("hdfs://slaver1:9000/"), true);
			while(listFiles.hasNext()){
				LocatedFileStatus next = listFiles.next();
				System.out.println("accessTime:"+ next.getAccessTime());
				System.out.println("owner:" + next.getOwner());
				System.out.println("blockSize:" + next.getBlockSize());
				System.out.println("path:" + next.getPath());
			}
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
	}
	
	
	@Test
	public void ioUpload(){
		try {
			FileInputStream input = new FileInputStream("E://20180507.txt");
			FSDataOutputStream output = fs.create(new Path("/201805/20180508.txt"));
			IOUtils.copy(input, output);
			System.out.println("文件上传结束......");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//input 读  output 写
	@Test
	public void iodownload(){
		try {
			FileOutputStream output = new FileOutputStream("E://20180507");
			FSDataInputStream input = fs.open(new Path("/201805/20180508"));
			IOUtils.copy(input, output);
			System.out.println("文件下载结束......");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	@Test
	public void ioRandom(){
		try {
			//input 读
			FSDataInputStream input = fs.open(new Path("/201805/20180508"));
			
			//
			input.seek(50000);
			
			//output 写
			FileOutputStream output = new FileOutputStream("E://20180508");
			
			IOUtils.copy(input, output);
			System.out.println("文件读取指定文件......");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Test
	public void cat(){
		try {
			//input 读
			FSDataInputStream input = fs.open(new Path("/201805/20180507.txt"));
			
			//
			//input.seek(50000);
			
			//output 写
			//FileOutputStream output = new FileOutputStream("E://20180508");
			
			IOUtils.copy(input, System.out);
			//org.apache.hadoop.io.IOUtils.copyBytes(input, System.out, 5000);
			
			System.out.println("文件读取结束......");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
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
