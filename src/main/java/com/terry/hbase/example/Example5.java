package com.terry.hbase.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import utils.SampleGenerator;

/**
 * Example 5. Bulk input with Mutator
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
 * $ mvn compile exec:java -Dexec.mainClass=com.terry.hbase.example.Example5 \ 
 * -Dbigtable.project=[your project id] -Dbigtable.instance=[your instance id]
 */
public class Example5 {
	final static int MAX_THREAD = 100;
	final static int STEPS = 1000;

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
			int localCount=0;
			BufferedMutator bufferedMutator = null;

			try {
				
				// define Mutator error handler
				final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
					@Override
					public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
						for (int i = 0; i < e.getNumExceptions(); i++) {
							System.out.println("Failed to sent put " + e.getRow(i) + ".");
						}
					}
				};
				// define mutator params
				BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf("contact"))
						.listener(listener);
				
				// create mutator 
				bufferedMutator = conn.getBufferedMutator(params);
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

				try {
					bufferedMutator.mutate(put);
					synchronized(this){
						count++;
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}

			try {
			      bufferedMutator.close();
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
		System.out.println("Steps per thread "+STEPS);
		ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD);
		Example5 example = new Example5();
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
		System.out.println("total record :"+Example5.count);
		System.out.println("Elapsed time :"+totalTime);
		conn.close();
		System.out.println("Finished all threads");
		System.exit(1);
	}

}
