package com.busap.comparator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


/**
 *  µœ÷Ωµ–Ú≈≈–Ú
 * @author zhoushuai
 *
 */
public class Job20140730to0812Comparator3  extends WritableComparator  {

	public Job20140730to0812Comparator3(){
		super(IntWritable.class,true);
	}
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		IntWritable atx = (IntWritable) a;
		IntWritable btx = (IntWritable) b;
		return btx.compareTo(atx);
	}


	
}
