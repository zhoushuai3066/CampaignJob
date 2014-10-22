package com.busap.job;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.chain.ChainReducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.busap.comparator.Job20140730to0812Comparator3;
import com.busap.map.Job20140730to0812Map1;
import com.busap.map.Job20140730to0812Map2;
import com.busap.map.Job20140730to0812Map3;
import com.busap.map.Job20140730to0812Map3_SORT;
import com.busap.map.Job20140902Map;
import com.busap.reduce.Job20140730to0812Reduce1;
import com.busap.reduce.Job20140730to0812Reduce3;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;


/**
 * create table ut1 (userId INT,phone STRING,name STRING,rtime STRING) row format delimited fields terminated by ',';
create table ut2 (phone STRING) row format delimited fields terminated by ',';

LOAD DATA INPATH '/tmp/job1-0730-0812/part-r-00000' OVERWRITE INTO TABLE ut1;
LOAD DATA LOCAL INPATH '/home/hive/REGP_KEY.txt' OVERWRITE INTO TABLE ut2;

select ut1.userId,ut1.name,ut1.rtime  from ut1 left semi join ut2 on ut1.phone = ut2.phone   53335  53858


select * from (select ut1.userId,ut1.name,ut1.rtime,ut2.phone from ut1 left outer join ut2 on ut1.phone = ut2.phone ) t1 where t1.phone is null;  523


select * from (select ut1.userId,ut1.name,ut1.rtime,ut1.phone from ut2 left outer join ut1 on ut1.phone = ut2.phone ) t1 where t1.phone is null;
 * @author zhoushuai
 *
 */
public class Job20140902{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		/**
		 * job1��ʼ
		 */
		Path outFile1 = new Path("/tmp/job1-0730-0812");
		Configuration jobconf1 = new Configuration();
		jobconf1.set("mapred.textoutputformat.separator", ",");
//		MongoConfigUtil.setInputURI(jobconf1, "mongodb://192.168.108.147:27017/mkdb.muser_relation");
		MongoConfigUtil.setInputURI(jobconf1, "mongodb://10.30.10.54:30000/mkdb.muser_info");
		org.apache.hadoop.mapreduce.Job job1 = new org.apache.hadoop.mapreduce.Job(jobconf1,"job1-0730-0812");
		job1.setJarByClass(Job20140902.class);
		FileOutputFormat.setOutputPath(job1, outFile1);
		job1.setMapperClass(Job20140902Map.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		job1.setInputFormatClass(MongoInputFormat.class);  
		job1.setOutputFormatClass(TextOutputFormat.class );
		
		
		
		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());  
		controlledJob1.setJob(job1);
		
		
		JobControl jc = new JobControl("chainJob");  
		jc.addJob(controlledJob1);
		
		
		 Thread jcThread = new Thread(jc);  
		 jcThread.start();  
		 boolean flag = true;
		 while(flag){  
			 if(jc.allFinished()){  
				  System.out.println(jc.getSuccessfulJobList());  
				  jc.stop();  
				  flag = false;
			 }
			 if(jc.getFailedJobList().size() > 0){ 
				  System.out.println(jc.getFailedJobList());  
				  jc.stop();  
				  flag = false;
			 }
		 }
//		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}

}
