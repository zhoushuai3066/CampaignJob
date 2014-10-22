package com.busap.map;


import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;


public class Job20140730to0812Map2 extends Mapper<Object, BSONObject, Text, Text>{
	
	private static final Log LOG = LogFactory.getLog(Job20140730to0812Map2.class);
	private Date begin;
	private Date end;
	
	private String keyword = "天上掉馅饼";
//	private String keyword = "的";
	
	
	
@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		
		LOG.info("zhoushuai---setup");
		try {
			begin = sdf.parse("2014-07-30 00:00:00");
			end = sdf.parse("2014-08-13 00:00:00");
			
//			begin = sdf.parse("2014-05-01 00:00:00");
//			end = sdf.parse("2014-08-22 00:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		super.setup(context);
	}



protected void map(Object key,BSONObject value,Context context) throws IOException, InterruptedException{
	LOG.info("zhoushuai---value"+value);
	Integer uid = (Integer) value.get("uid");
	LOG.info("zhoushuai---uid"+uid);
	String content =  (String) value.get("content");
	LOG.info("zhoushuai---content"+content);
	Date createTime = (Date) value.get("createTime");
	LOG.info("zhoushuai---createTime"+createTime);
	if(createTime!=null){
		System.out.println(createTime+"-----zhoushaui");
		if(createTime.before(end)&&createTime.after(begin)){
			if(content!=null){
				if(content.indexOf(keyword)>=0){
					context.write(new Text(uid.toString()),new Text("Y"));
				}
			}
		}
	}
//	    ClassDumperAgent classDumperAgent = new ClassDumperAgent();
	}



//  public static void main(String[] args) throws ParseException{
//	  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	  SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//	  Date start = sdf.parse("2010-05-01 00:00:00");
//	  Date end = sdf.parse("2010-05-05 00:00:00");
//	  Date test = sdf2.parse("2010/05/04 23:59:59");
//      System.out.println(test.before(end));
//      System.out.println(test.after(start));
//      
//      if(test.before(end)&&test.after(start)){
//    	  System.out.println(1);
//      }
//      
//      String word = "天上掉馅饼";
//      String testword = "天上掉馅饼hello 双方都爽肤水鼎飞丹砂";
//      System.out.println(testword.indexOf(word));
//      
//  }
}
