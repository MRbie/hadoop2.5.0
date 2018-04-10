package com.bie.hbase.connection;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * 
 * @author 别先生
 * 1、在web应用中，如果我们之间使用htable来操作hbase，那么在创建连接和关闭连接的时候，一定会浪费资源。
 * 	那么hbase提供了一个连接池的基础。主要设计到的类和解耦包括。HConnection,HConnectionManager,HTableInterface,ExecutorService四个。
 *  其中HConnection就是hbase封装好的hbase连接池，HConnectionManager是管理连接池的一个类。HTableInterface是在类HTable的基础上进行的一个抽象接口。
 *  ExecutorService是jdk的线程池对象。
 */
public class HbaseConnection {

	public static void HbaseConnectionPool(Configuration conf){
		ExecutorService pool = Executors.newFixedThreadPool(2);
		try {
			HConnection createConnection = HConnectionManager.createConnection(conf);
			HTableInterface table = createConnection.getTable("");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
