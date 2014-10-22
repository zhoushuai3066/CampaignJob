package com.busap.map;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.bson.BSONObject;

public class Job20140730to0812Map3_SORT extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable>{



protected void map(IntWritable key,IntWritable value,Context context) throws IOException, InterruptedException{
		context.write(key,value);
	}

}
