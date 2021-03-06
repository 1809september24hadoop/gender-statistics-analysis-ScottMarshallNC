package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IncreaseInEducationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	int count = 1;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String line = value.toString();
		StringBuilder newLine = new StringBuilder();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				if((word.indexOf("United States") != -1)
						&&(word.indexOf("completed")!=-1)
						&&(word.indexOf("Education")!=-1)
						&&(word.indexOf("female")!=-1)){
					String[] parts = word.split("\",");
					for(int i=0; i < parts.length; i++){
						parts[i].trim();
						newLine.append(parts[i]);
						if(i != parts.length-1){
							newLine.append(";");
						}
					}
					String a = newLine.toString();
					String s = a.replaceAll("\"", "");
					context.write(new Text(s), new IntWritable(count++));
				}
			}
		}
	}
}