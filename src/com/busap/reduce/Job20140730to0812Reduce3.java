package com.busap.reduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Job20140730to0812Reduce3 extends Reducer<Text, Text, IntWritable, IntWritable> {
	
	protected void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		Iterator<Text> it = values.iterator();
		boolean hasY = false;
		String count = null;
		while(it.hasNext()){
			Text value = it.next();
			if(hasY&&count!=null){
				break;
			}else{
				String vt = value.toString();
				if(vt.indexOf("Y")>=0){
					hasY = true;
				}else{
					count = vt;
				}
			}
		}
		
		if(hasY){
//			context.write(new IntWritable(Integer.parseInt(key.toString())),count==null?new IntWritable(0):new IntWritable(Integer.parseInt(count)));
			context.write(count==null?new IntWritable(0):new IntWritable(Integer.parseInt(count)),new IntWritable(Integer.parseInt(key.toString())));
		}
	}
	
	
}
