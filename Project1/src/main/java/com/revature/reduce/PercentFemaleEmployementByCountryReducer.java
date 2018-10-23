package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PercentFemaleEmployementByCountryReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

	int count = 1;
	public void reduce(Text value, Iterable<IntWritable> key, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				if((word.indexOf("Employment to pop") != -1)
						&&(word.indexOf("ILO")!=-1)
						&&(word.indexOf("female")!=-1)
						&&(word.indexOf("15+")!=-1)
						&&(word.indexOf("WLD")!=-1)){
					
					String[] tokens = word.split(";");
					
						if((isDouble(tokens[44]))&&(isDouble(tokens[59]))){
							double educationRateOlder = Double.parseDouble(tokens[44]);
							double educationRateNewer = Double.parseDouble(tokens[59]);
							Double educationNew = educationRateNewer - educationRateOlder; 
							int sub = 2000; 
							int subSecond = 2015;
							String s = tokens[0] + " " + tokens[2] + " " + sub +" to " + subSecond;
								context.write(new Text(s), new DoubleWritable(educationNew));
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
