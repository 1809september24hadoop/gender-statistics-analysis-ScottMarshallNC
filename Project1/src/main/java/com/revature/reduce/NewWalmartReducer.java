package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NewWalmartReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	int count = 1;
	public void reduce(Text value, Iterable<IntWritable> key, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
			
			String[] tokens = word.split(";");
			int counter = 0;

			for(int i =44; i < tokens.length - 1; i++){
				int j = i + 1;
				if((isDouble(tokens[i]))&&(isDouble(tokens[j]))){
					double educationRateOlder = Double.parseDouble(tokens[i]);
					double educationRateNewer = Double.parseDouble(tokens[j]);
					Double educationNew = educationRateNewer - educationRateOlder; 
					int sub = i + 1956; 
					int subSecond = j + 1956;
					if(educationRateNewer > educationRateOlder){
						counter++;
						if((counter > 3)&&(subSecond > 2014)&&(educationNew > 1.0)){
							
								String s ="POSSIBLE WALMART LOCATION!" + tokens[0] + " " + sub +" to " + subSecond ;
							
							context.write(new Text(s), new DoubleWritable(educationNew));
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