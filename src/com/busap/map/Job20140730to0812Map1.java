package com.busap.map;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class Job20140730to0812Map1 extends Mapper<Object, BSONObject, Text, IntWritable>{
	
protected void map(Object key,BSONObject value,Context context) throws IOException, InterruptedException{
		
	Integer uid = (Integer) value.get("uid");
//	    Integer followid =  (Integer) value.get("followid");
	    context.write(new Text(uid.toString()), new IntWritable(1));
//	    ClassDumperAgent classDumperAgent = new ClassDumperAgent();
	}

}
