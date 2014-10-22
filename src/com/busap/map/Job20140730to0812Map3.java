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

@SuppressWarnings("unused")
public class Job20140730to0812Map3 extends Mapper<Object, Text, Text, Text>{



protected void map(Object key,Text value,Context context) throws IOException, InterruptedException{
		String[] words = value.toString().split(",");
		context.write(new Text(words[0]), new Text(words[1]));
	}

}
