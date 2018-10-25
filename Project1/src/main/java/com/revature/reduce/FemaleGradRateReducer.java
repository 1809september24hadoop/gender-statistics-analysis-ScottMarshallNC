package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FemaleGradRateReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	int count = 1;
	public void reduce(Text value, Iterable<IntWritable> key, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
						String[] tokens = word.split(";");
						for(int i=tokens.length-1; i > 0; i--){
							if(isDouble(tokens[i])){
								double educationRate = Double.parseDouble(tokens[i]);
								if((educationRate < 30.0)&&(educationRate >= 0)){
									int j = i + 1956;
									String s = tokens[0] + " most recent female tertiary graduation rate was in " + j + ": ";
									context.write(new Text(s), new DoubleWritable(educationRate));
									break;
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

