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
import com.busap.reduce.Job20140730to0812Reduce1;
import com.busap.reduce.Job20140730to0812Reduce3;
import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class Job20140730to0812{

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		
		/**
		 * job1开始
		 */
		Path outFile1 = new Path("/tmp/job1-0730-0812");
		Configuration jobconf1 = new Configuration();
		jobconf1.set("mapred.textoutputformat.separator", ",");
//		MongoConfigUtil.setInputURI(jobconf1, "mongodb://192.168.108.147:27017/mkdb.muser_relation");
		MongoConfigUtil.setInputURI(jobconf1, "mongodb://10.30.10.54:30000/mkdb.muser_relation");
		org.apache.hadoop.mapreduce.Job job1 = new org.apache.hadoop.mapreduce.Job(jobconf1,"job1-0730-0812");
		job1.setJarByClass(Job20140730to0812.class);
		FileOutputFormat.setOutputPath(job1, outFile1);
		job1.setMapperClass(Job20140730to0812Map1.class);
		job1.setReducerClass(Job20140730to0812Reduce1.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		job1.setInputFormatClass(MongoInputFormat.class);  
		job1.setOutputFormatClass(TextOutputFormat.class );
		
		//如果需要依赖提交，则使用下面代码，通过dependindjob可以设置依赖提交。也就是根据依赖关系提交job，如果不使用dependingjob则会将所有job都一次性提交。也会依赖执行，但是会先占用资源。
//		JobConf jcfg1 = new JobConf();
//		Job cjob1 = new Job(jcfg1);
//		cjob1.setJob(job1);
		/**
		 * job1结束
		 */
		
	
		
		/**
		 * job2 开始
		 */
		Path outFile2 = new Path("/tmp/job2-0730-0812");
		Configuration jobconf2 = new Configuration();
		jobconf2.set("mapred.textoutputformat.separator", ",");
//		MongoConfigUtil.setInputURI(jobconf2, "mongodb://192.168.108.147:27017/mkdb.mrecommend");
		MongoConfigUtil.setInputURI(jobconf2, "mongodb://10.30.10.54:30000/mkdb.mrecommend");
		org.apache.hadoop.mapreduce.Job job2 = new org.apache.hadoop.mapreduce.Job(jobconf2,"job2-0730-0812");
		job2.setJarByClass(Job20140730to0812.class);
		FileOutputFormat.setOutputPath(job2, outFile2);
		job2.setMapperClass(Job20140730to0812Map2.class);
//		job2.setReducerClass(Job20140730to0812Reduce1.class);
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);
		
		job2.setInputFormatClass(MongoInputFormat.class);  
		job2.setOutputFormatClass(TextOutputFormat.class );
		
		
		//如果需要依赖提交，则使用下面代码，通过dependindjob可以设置依赖提交。也就是根据依赖关系提交job，如果不使用dependingjob则会将所有job都一次性提交。也会依赖执行，但是会先占用资源。
//		JobConf jcfg2 = new JobConf();
//		Job cjob2 = new Job(jcfg2);
//		cjob2.setJob(job2);
//		cjob2.addDependingJob(cjob1);
		/**
		 * job2 结束
		 */
		
		/**
		 * job3 开始
		 */
		Path outFile3 = new Path("/tmp/job3-0730-0812");
		Configuration jobconf3 = new Configuration();
		jobconf3.set("mapred.textoutputformat.separator", ",");
		org.apache.hadoop.mapreduce.Job job3 = new org.apache.hadoop.mapreduce.Job(jobconf3,"job3-0730-0812");
		job3.setJarByClass(Job20140730to0812.class);
		FileOutputFormat.setOutputPath(job3, outFile3);
//		job3.setMapperClass(Job20140730to0812Map3.class);
		job3.setReducerClass(Job20140730to0812Reduce3.class);
		job3.setMapOutputKeyClass(Text.class);
		job3.setMapOutputValueClass(Text.class);
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(IntWritable.class);
		ChainReducer cr = new ChainReducer();
		cr.setReducer(job3, Job20140730to0812Reduce3.class, Text.class, Text.class, IntWritable.class, IntWritable.class, jobconf3);
		cr.addMapper(job3, Job20140730to0812Map3_SORT.class, IntWritable.class, IntWritable.class, IntWritable.class, IntWritable.class, jobconf3);
//		job3.setGroupingComparatorClass(Job20140730to0812Comparator3.class);
//		job3.setSortComparatorClass(Job20140730to0812Comparator3.class);
		job3.setNumReduceTasks(1);   
		
		MultipleInputs.addInputPath(job3, outFile1, TextInputFormat.class,Job20140730to0812Map3.class);
		MultipleInputs.addInputPath(job3, outFile2, TextInputFormat.class, Job20140730to0812Map3.class);
		
		
		//如果需要依赖提交，则使用下面代码，通过dependindjob可以设置依赖提交。也就是根据依赖关系提交job，如果不使用dependingjob则会将所有job都一次性提交。也会依赖执行，但是会先占用资源。
//		JobConf jcfg2 = new JobConf();
//		Job cjob2 = new Job(jcfg2);
//		cjob2.setJob(job2);
//		cjob2.addDependingJob(cjob1);
		/**
		 * job3 结束
		 */
		
		
		
		
		ControlledJob controlledJob1 = new ControlledJob(job1.getConfiguration());  
		controlledJob1.setJob(job1);
		
		ControlledJob controlledJob2 = new ControlledJob(job2.getConfiguration());  
		controlledJob2.setJob(job2);
		
//		ControlledJob controlledJob3 = new ControlledJob(job2.getConfiguration());  
//		controlledJob3.setJob(job3);
		
		JobConf jcfg3 = new JobConf();
		Job cjob3 = new Job(jcfg3);
		cjob3.setJob(job3);
		cjob3.addDependingJob(controlledJob1);
		cjob3.addDependingJob(controlledJob2);
		
		JobControl jc = new JobControl("chainJob");  
		jc.addJob(controlledJob1);
		jc.addJob(controlledJob2);
		jc.addJob(cjob3);
		
		
		
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
