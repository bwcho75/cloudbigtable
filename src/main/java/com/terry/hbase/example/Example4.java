package com.terry.hbase.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Stack;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import utils.SampleGenerator;

/**
 * Example 4. Batch Bulk insert with table.put(List<Put>) 
 * @author terrycho
 * Before run this code, you need to create table 
 * Here is instruction to create table
 * $ echo project = [your project id] > ~/.cbtrc
 * $ echo instance = [your instance id] >> ~/.cbtrc
 * $ cat ~/.cbtrc
 * project = terrycho-sandbox
 * instance = terrycho-bigtable
 * 
 * $ cbt createfamily contact contact
 * $ cbt ls contact
 * $ cbt read contact
 * 
 * Compile & run instruction
 * $ mvn compile exec:java -Dexec.mainClass=com.terry.hbase.example.Example4 \ 
 * -Dbigtable.project=[your project id] -Dbigtable.instance=[your instance id]
 */
public class Example4 {
	final static int MAX_THREAD = 100;
	final static int STEPS = 1000;
	final static int BATCH_SIZE=100;
	
	SampleGenerator gen = new SampleGenerator();
	public static int count = 0;
	
	// CBT Connection Pool
	
	public class WriterThread implements Runnable{
		int steps = 0;
		String projectId;
		String instanceId;
		Connection conn;
		
		public WriterThread(Connection conn,int steps){
			this.conn = conn;
			this.steps = steps;
		}
		
		public void run(){
		    Table table = null;
		    List <Put>puts = new ArrayList<Put>();
		    int localCount=0;
		    
			try {
				table = conn.getTable(TableName.valueOf("contact"));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				System.out.println("Get table error");
				e1.printStackTrace();
			}
		    
			for(int i =0;i<steps;i++){
				String rowKey = gen.getString(64);
				String cfName = "name";
				String cfContact = "contact";
				String lastName = gen.getString(5);
				String firstName = gen.getString(10);
				String mobile = gen.getPhone();
				String phone = gen.getPhone();
				String email = gen.getString(10)+"@"+gen.getString(5)+".com";
				
			    Put put = new Put(Bytes.toBytes(rowKey));
			    // CF name
			    put.addColumn(cfName.getBytes(), Bytes.toBytes("lastname"), Bytes.toBytes(lastName));
			    put.addColumn(cfName.getBytes(), Bytes.toBytes("firstname"), Bytes.toBytes(firstName));
			    
			    // CF contact
			    put.addColumn(cfContact.getBytes(), Bytes.toBytes("mobile"), Bytes.toBytes(mobile));
			    put.addColumn(cfContact.getBytes(), Bytes.toBytes("phone"), Bytes.toBytes(phone));
			    put.addColumn(cfContact.getBytes(), Bytes.toBytes("email"), Bytes.toBytes(email));
			    
			    puts.add(put);
			    localCount++;
			    if(localCount == BATCH_SIZE){
				    // System.out.println(Thread.currentThread().getName()+this+" key :"+rowKey);  
				    try {
						table.put(puts);
						synchronized(this){
							count+=BATCH_SIZE;
						}

					} catch (IOException e) {
						// TODO Auto-generated catch block
						System.out.println("Put value error");
						e.printStackTrace();
					}
				    puts.clear();
				    localCount = 0;
			    }
			}
			
			try {
				table.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println("Connection close error");
				e.printStackTrace();
			}
		}
	}

	public static void main(String args[]) throws IOException{
		String projectId = System.getProperty("bigtable.project");
		String instanceId = System.getProperty("bigtable.instance");

		System.out.println("Creating Thread "+MAX_THREAD);
		System.out.println("Steps per thread"+STEPS);
		System.out.println("Batch Size "+BATCH_SIZE);
		ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD);
		Example4 example = new Example4();
		Connection conn = BigtableConfiguration.connect(projectId, instanceId);

		long startTime = System.currentTimeMillis() ;

		for(int i=0;i<MAX_THREAD;i++){
			Runnable writer = example.new WriterThread(conn,STEPS);
			executor.execute(writer);
		}

		// shutdown thread
		executor.shutdown();
		while (!executor.isTerminated()) {   }  
		long totalTime = System.currentTimeMillis() - startTime;
		System.out.println("total record :"+Example4.count);
		System.out.println("Elapsed time :"+totalTime);
		conn.close();
		System.out.println("Finished all threads");
		System.exit(1);
	}

}
