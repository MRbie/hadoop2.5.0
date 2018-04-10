package com.bie.hbase.ddl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.bie.hbase.utils.HbaseUtils;

/**
 * 
 * @author 别先生
 * 1、Java客户端其实就是shell客户端的一种实现，操作命令基本上就是shell客户端命令的一个映射。
 * 	Java客户端使用的配置信息是被映射到一个HBaseConfiguration的实力对象中的，当使用该类的create方法创建对象的时候
 * 	会从classpath路径下获取hbase-site.xml文件并进行配置文件内容的读取，同事会读取hadoop的配置文件信息。
 *  也可以通过java代码指定命令信息，只需要给定zk的相关环境变量信息即可。
 * 2、HbaseAdmin类是主要进行DDL操作相关的一个接口类，主要包括命名空间管理，用户表管理。通过该接口我们可以创建，删除，获取用户表，也可以
 * 	进行用户表的分割，紧缩等操作。
 * 3、HTable是hbase中的用户表的一个映射的java实例，我们可以通过该类进行表数据的操作包括数据的增删改查，也就是在
 * 我们可以类似shell的put,get,scan进行数据的操作。
 * 4、HTableDescriptor是hbase用户表的具体描述信息类，一般我们创建表获取表信息，就是通过该类进行的。
 * 5、put类是专门提供插入数据的类。
 * 6、Get类是专门提供根据rowkey获取数据的类。
 * 7、scan是专门进行范围查找的类。
 * 8、delete是专门进行删除的类。
 */
public class HbaseCreateTable {
	
	/***
	 * 创建hbase数据表的方法
	 * @param hBaseAdmin
	 */
	public static void hbaseCreateTable(HBaseAdmin hBaseAdmin){
		//创建一个tableName对象,给定数据表名称
		TableName tableName = TableName.valueOf("student");
		//HTableDescriptor描述信息类
		HTableDescriptor htd = new HTableDescriptor(tableName);
		//给定数据表的family
		htd.addFamily(new HColumnDescriptor("f"));
		try {
			//如果数据表不存在就创建,否则不创建
			if(!hBaseAdmin.tableExists(tableName)){
				hBaseAdmin.createTable(htd);
			}else{
				System.out.println("数据表已经存在......");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(tableName + "数据表创建成功了......");
	}
	
	
	/**
	 * 获取hbase的数据表信息
	 * @param hBaseAdmin
	 */
	public static void hbaseTableDescriptor(HBaseAdmin hBaseAdmin){
		TableName tableName = TableName.valueOf("student");
		try {
			HTableDescriptor htd = hBaseAdmin.getTableDescriptor(tableName);
			if(hBaseAdmin.tableExists(tableName)){
				System.out.println(htd.toString());
			}else{
				System.out.println("表不存在......");
			}
		} catch (TableNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("获取到了数据表的信息......");
	}
	
	/***
	 * 删除数据表的信息
	 * @param hBaseAdmin
	 */
	public static void deleteHbaseTable(HBaseAdmin hBaseAdmin){
		TableName tableName = TableName.valueOf("student");
		try {
			//删除之前需要禁用数据表
			hBaseAdmin.disableTable(tableName);
			//如果数据表存在就删除
			if(hBaseAdmin.tableExists(tableName)){
				//如果表禁用了，才可以删除
				if(hBaseAdmin.isTableDisabled(tableName)){
					hBaseAdmin.deleteTable(tableName);
					System.out.println("数据表删除了......");
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * hbase数据表的字段插入
	 * @param hBaseAdmin
	 */
	public static void hbaseTablePut(Configuration conf, HTable htTable){
		//获取到数据表的名称
		TableName tableName = TableName.valueOf("student");
		Put puts = new Put(Bytes.toBytes("row1"));
		Put puts2 = new Put(Bytes.toBytes("row2"));
		Put puts3 = new Put(Bytes.toBytes("row3"));
		Put puts4 = new Put(Bytes.toBytes("row4"));
		
		try {
			//根据数据表名称对数据表进行插入操作
			htTable = new HTable(conf, tableName);
			//设置列名称和数据值
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("1"));
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("张三"));
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("sex"), Bytes.toBytes("男"));
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes("20"));
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("phone"), Bytes.toBytes("15236083001"));
			puts.add(Bytes.toBytes("f"), Bytes.toBytes("email"), Bytes.toBytes("1748741328@qq.com"));
			//将列名称和数据进行插入操作
			htTable.put(puts);
		
			puts2.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("2"));
			puts2.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("lisi"));
			
			puts3.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("3"));
			puts3.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("wangwu"));
			
			puts4.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("4"));
			puts4.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("zhaoliu"));
			
			List<Put> list = new ArrayList<Put>();
			list.add(puts2);
			list.add(puts3);
			list.add(puts4);
			htTable.put(list);
			System.out.println("插入成功.....");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * 获取数据表的信息
	 * @param conf
	 * @param htTable
	 */
	public static void hbaseTableGet(Configuration conf, HTable htTable){
		TableName tableName = TableName.valueOf("student");
		try {
			htTable = new HTable(conf, tableName);
			//获取到数据表的信息
			Get get = new Get(Bytes.toBytes("row1"));
			Result result = htTable.get(get);
			//获取到数据表的信息
			byte[] id = result.getValue(HbaseUtils.family, Bytes.toBytes("id"));
			byte[] name = result.getValue(HbaseUtils.family, Bytes.toBytes("name"));
			byte[] email = result.getValue(HbaseUtils.family, Bytes.toBytes("email"));
				
			System.out.println("id:" + Bytes.toString(id));
			System.out.println("name:" + Bytes.toString(name));
			System.out.println("email:" + Bytes.toString(email));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/***
	 * 数据表删除的方法
	 * @param conf
	 * @param htTable
	 */
	public static void hbaseTableDelete(Configuration conf, HTable htTable){
		TableName tableName = TableName.valueOf("student");
		try {
			htTable = new HTable(conf, tableName);
			Delete delete = new Delete(Bytes.toBytes("row1"));
			delete.deleteColumn(HbaseUtils.family, Bytes.toBytes("id"));
			delete.deleteColumn(HbaseUtils.family, Bytes.toBytes("name"));
			htTable.delete(delete);
			System.out.println("删除操作结束......");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	/**
	 * scan的用法
	 * @param conf
	 * @param htTable
	 */
	public static void hbaseTableScan(Configuration conf, HTable htTable){
		TableName tableName = TableName.valueOf("student");
		Scan scan = new Scan();
		//可以设置起始行
		scan.setStartRow(Bytes.toBytes("row1"));
		scan.setStopRow(Bytes.toBytes("row5"));
		try {
			htTable = new HTable(conf, tableName);
			ResultScanner rs = htTable.getScanner(scan);
			Iterator<Result> iterator = rs.iterator();
			Result result = null;
			while(iterator.hasNext()){
				result = iterator.next();
				printScanResult(result);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * scan的打印结果
	 */
	public static void printScanResult(Result result){
		//打印结果
		System.out.println("******************************************");
		NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
		for(Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()){
			String family = Bytes.toString(entry.getKey());
			for(Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : entry.getValue().entrySet()){
				//获取到列名称
				String column = Bytes.toString(columnEntry.getKey());
				String value = Bytes.toString(columnEntry.getValue().firstEntry().getValue());
				//String value = "";
				//if("age".equals(column)){
					//value = "" + Bytes.toInt(columnEntry.getValue().firstEntry().getValue());
				//}else{
					//value = Bytes.toString(columnEntry.getValue().firstEntry().getValue());
				//}
				System.out.println("family:" +family + ",列名称:" +column + ",值:" + value);
			}
		}
		System.out.println("******************************************");
	}
	
	public static void main(String[] args) {
		//获取到hbase的配置信息
		Configuration conf = HbaseUtils.getHbaseConfiguration();
		HBaseAdmin hbaseAdmin = null;
		//
		HTable htTable = null;
		try {
			//HbaseAdmin对象
			hbaseAdmin = new HBaseAdmin(conf);
			//1、创建hbase数据表的方法
			//hbaseCreateTable(hbaseAdmin);
		    //2、获取到数据表的信息
			//hbaseTableDescriptor(hbaseAdmin);
			//3、删除数据表的信息
			//deleteHbaseTable(hbaseAdmin);
			
			//4、数据表的插入操作
			//hbaseTablePut(conf, htTable);
			//5、获取到数据表的信息
			//hbaseTableGet(conf, htTable);
			//6、删除数据表信息
			//hbaseTableDelete(conf, htTable);
			//7、scan的使用
			hbaseTableScan(conf, htTable);
			
			
			//最后释放资源
			hbaseAdmin.close();
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
}
