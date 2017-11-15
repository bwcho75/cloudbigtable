package com.terry.hbase.example;

import java.io.IOException;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;


/*
 *  This is HBase Basic Example
 *  It combines create connection, create table and simple put & get
 *  mvn compile exec:java -Dexec.mainClass=com.terry.hbase.example.Example1  \
 *   -Dbigtable.project=terrycho-sandbox 
 *   -Dbigtable.instance=terrycho-bigtable
 */

public class Example1 {
	
	static Connection getConnection(){
	    String projectId = System.getProperty("bigtable.project");
	    String instanceId = System.getProperty("bigtable.instance");
	    Connection conn = BigtableConfiguration.connect(projectId, instanceId);
	    
	    return conn;
	}
	
	static void log(String str){
		System.err.println(str);
	}
	
	public static void main(String args[]) throws IOException{
	    String tableName = "example1";
	    String cfName = "mycf";
	    String colName = "mycol";
	    Connection conn = getConnection();
	    
	    // step 1. create table
	    try {
			Admin admin = conn.getAdmin();
			
			// define table schema
			HTableDescriptor desc = new
					HTableDescriptor(TableName.valueOf(tableName));
			desc.addFamily(new HColumnDescriptor(cfName));
			
			admin.createTable(desc);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			log("Table creation error");
			e.printStackTrace();
		}
	    
	    // step 2. put value
	    Table table = conn.getTable(TableName.valueOf(tableName));
	    
	    // define row
	    byte [] rowKey = Bytes.toBytes("rowkey1");
	    Put put = new Put(rowKey);
	    put.addColumn(cfName.getBytes(), colName.getBytes(), Bytes.toBytes("Cell Value"));
	    
	    // insert row
	    table.put(put);
		
	    // step 3 retrieve value
	    Get g = new Get(rowKey);
	    Result r = table.get(g);
	    byte[] value = r.getValue(cfName.getBytes(), colName.getBytes());
	    String strValue = Bytes.toString(value);
	    
	    log("Get value :"+strValue);
	    
	    table.close();
	    conn.close();
	    
	}

}
