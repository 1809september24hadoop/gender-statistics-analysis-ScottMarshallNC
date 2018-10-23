package com.revature.map;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FemaleGraduationMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	int count = 1;
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException{
		
		String line = value.toString();
		StringBuilder newLine = new StringBuilder();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				if((word.indexOf("graduation ratio") != -1)
						&&(word.indexOf("tertiary")!=-1)
						&&(word.indexOf("female")!=-1)){
					String[] parts = word.split("\",");
					for(int i=0; i < parts.length; i++){
						parts[i].trim();
						if(i != parts.length){
							newLine.append(parts[i]);
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
