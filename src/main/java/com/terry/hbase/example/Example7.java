package com.terry.hbase.example;

import java.io.IOException;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;


/*
 *  Example 7. Simple HBase Prefix filter and Scanner example
 *  mvn compile exec:java -Dexec.mainClass=com.terry.hbase.example.Example7  \
 *   -Dbigtable.project=[project id]
 *   -Dbigtable.instance=[cbt instance name]
 */

public class Example7 {
	
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
	    String tableName = "contact";
	    String prefix = System.getProperty("filter.prefix");

	    // get table
	    Connection conn = getConnection();
	    Table table = conn.getTable(TableName.valueOf(tableName));
	    
	    // create prefix filter
	    Filter prefixFilter = new PrefixFilter(Bytes.toBytes(prefix));
	    Scan scan = new Scan();
	    scan.setFilter(prefixFilter);
	    ResultScanner scanner = table.getScanner(scan);
	    for(Result result : scanner){
	    	System.out.println("Key :"+Bytes.toString(result.getRow()));
	    	for(Cell cell : result.rawCells()){
	    		System.out.print("CF "+Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()));
	    		System.out.print(", COL "+Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()));
	            System.out.println(", Value: "+Bytes.toString(cell.getValueArray(), cell.getValueOffset(),cell.getValueLength()));
	    	}
	    }
	    scanner.close();
	    
	    table.close();
	    conn.close();
	    
	    System.exit(1);
	    
	}

}
