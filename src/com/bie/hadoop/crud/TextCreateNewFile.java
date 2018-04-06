package com.bie.hadoop.crud;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.bie.hadoop.utils.HdfsUtils;

public class TextCreateNewFile {

	/**
	 * 在hdfs上面创建文件
	 * 指定绝对路径
	 */
	private static void createNewFile(){
		Configuration conf = new Configuration();
		//指定想要创建文件的ip地址和端口号
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		//获取到配置信息
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			//创建文件
			boolean createNewFile = fs.createNewFile(new Path("/201804/20180406/newFile.txt"));
			System.out.println(createNewFile ? "文件创建成功." : "文件创建失败.");
			//关闭fs
			fs.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	
	/**
	 * 在hdfs上面创建文件
	 * 指定相对路径，在当前用户user目录下面。
	 */
	private static void createNewFileRelative(){
		Configuration conf = new Configuration();
		//指定想要创建文件的ip地址和端口号
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		//获取到配置信息
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			//创建文件
			boolean createNewFile = fs.createNewFile(new Path("newFileRelative.txt"));
			System.out.println(createNewFile ? "文件创建成功." : "文件创建失败.");
			//关闭fs
			fs.close();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	
	/***
	 * 删除指定文件
	 */
	private static void deleteFile(){
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			//删除指定文件
			boolean deleteOnExit = fs.deleteOnExit(new Path("/201804/20180406/newFile.txt"));
			System.out.println(deleteOnExit ? "文件删除成功." : "文件删除失败.");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 文件追加内容
	 */
	private static void appendContext(){
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		FileSystem fs;
		try {
			fs = FileSystem.get(conf);
			Path path = new Path("/201804/20180406/newFile.txt");
			//输出就是写,输入就是读
			FSDataOutputStream dos = fs.append(path);
			dos.write("我喜欢学习".getBytes());
			System.out.println("文件追加内容成功......");
			dos.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 创建文件以后书写内容
	 */
	private static void createAndWrite(){
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://192.168.19.131:9000");
		try {
			FileSystem fs = FileSystem.get(conf);
			FSDataOutputStream dos = fs.create(new Path("/201804/20180406/createAndWrite.txt"));
			BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(dos));
			bw.write("我爱学习");
			bw.newLine();
			bw.write("我爱学习大数据");
			bw.newLine();
			System.out.println("文件创建成功且内容书写完毕");
			bw.close();
			dos.close();
			fs.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	/***
	 * 读取文件内容信息
	 */
	private static void openFile(){
		FileSystem fs = HdfsUtils.getFileSystem();
		try {
			//注意是绝对路径，不是相对路径
			InputStream open = fs.open(new Path("/201804/20180406/createAndWrite.txt"));
			BufferedReader br = new BufferedReader(new InputStreamReader(open));
			String line = null;
			while((line = br.readLine()) != null){
				System.out.println(line);
			}
			br.close();
			open.close();
			fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void mkdirsFile(){
		FileSystem fs = HdfsUtils.getFileSystem();
		try {
			boolean mkdirs = fs.mkdirs(new Path("/201804/20180406/201804061637"));
			System.out.println(mkdirs ? "文件创建成功." : "文件创建失败.");
			fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private static void copyFromLocalFile(){
		FileSystem fs = HdfsUtils.getFileSystem();
		try {
			fs.copyFromLocalFile(new Path("c:/log4j.properties2"), new Path("/201804/20180406"));
			System.out.println("文件上传成功");
			fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static void copyToLocalFile(){
		FileSystem fs = HdfsUtils.getFileSystem();
		try {
			fs.copyToLocalFile(new Path("/201804/20180406/log4j.properties2"), new Path("e:/log4j.properties2"));
			System.out.println("文件下载成功");
			fs.close();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 获取到文件的状态
	 */
	private static void getFileStatus(){
		FileSystem fs = HdfsUtils.getFileSystem();
		try {
			FileStatus fileStatus = fs.getFileStatus(new Path("/201804/20180406/log4j.properties2"));
			System.out.println(fileStatus.toString());
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args) {
		//1、测试创建文件是否成功。
		//createNewFile();
		//2、创建相对路径的文件
		//createNewFileRelative();
		//3、测试删除文件
		//deleteFile();
		//4、文件追加内容
		//appendContext();
		//5、创建文件并且书写内容
		//createAndWrite();
		//6、读取文件的内容信息
		//openFile();
		//7、创建文件
		//mkdirsFile();
		//8、上传文件
		//copyFromLocalFile();
		//9、下载文件
		//copyToLocalFile();
		//10、获取到文件的状态
		getFileStatus();
		
		
	}
	
	
}
