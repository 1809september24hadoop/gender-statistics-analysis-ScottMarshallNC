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
					
						String[] tokens = word.split(",");
						for(int i=tokens.length-1; i > 0; i--){
							tokens[i].trim();
							if(isDouble(tokens[i])){
								double educationRate = Double.parseDouble(tokens[i]);
								if(educationRate < 30.0){
									String s = tokens[0] + "most recent education rate is: " + tokens[i];
									context.write(new Text(s), new IntWritable(count++));
									break;
								}
							}
						}
				}
			}
		}
	}
	public boolean isDouble( String str ){
		  try{
		    Double.parseDouble( str );
		    return true;
		  }
		  catch( Exception e ){
		    return false;
		  }
	}
}


//String row = value.toString();
//String[] tokens = row.split("\t");
//String inab = tokens[0];
//String state = abMap.get(inab);
//outputKey.set(state);
//outputValue.set(row);
//context.write(outputKey, outputValue)
