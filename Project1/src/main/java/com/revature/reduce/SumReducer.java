package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	int count = 1;
	public void reduce(Text value, Iterable<IntWritable> key, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				if((word.indexOf("attain") != -1)
						&&(word.indexOf("female")!=-1)
						&&(word.indexOf("some primary")!=-1)){
					context.write(new Text(word), new IntWritable(count++));
				}
			}
		}
	}

}
