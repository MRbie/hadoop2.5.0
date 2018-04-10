package com.bie.hbase.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 
 * @author 别先生
 * Hbase的工具类
 */
public class HbaseUtils {

	public static byte[] family = Bytes.toBytes("f");
	
	public static Configuration getHbaseConfiguration(){
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "192.168.19.131,192.168.19.132,192.168.19.133");
		return conf;
	}
	
}
