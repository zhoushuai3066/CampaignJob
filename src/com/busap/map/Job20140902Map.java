package com.busap.map;


import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

public class Job20140902Map extends Mapper<Object, BSONObject, Text, Text>{
	
protected void map(Object key,BSONObject value,Context context) throws IOException, InterruptedException{
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	Integer uid = (Integer) value.get("_id");
	String phone = (String) value.get("phone");
	String name =  (String)value.get("name");
	Date registerTime = (Date) value.get("registerTime");
	    context.write(new Text(uid.toString()), new Text(phone+","+name+","+sdf.format(registerTime.getTime())));
	}



public static void main(String[] args){
	long a = 1409632134516L;
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	System.out.println(sdf.format(new Date(a)));
}

}
