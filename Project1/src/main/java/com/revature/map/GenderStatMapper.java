package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GenderStatMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	int count = 1;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				String s = word.replaceAll("\",\"", ",").replaceAll("\"", "").replaceAll("( )+", " ");
				context.write(new Text(s), new IntWritable(count++));
			}
		}
	}
}
