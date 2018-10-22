package com.revature.reduce;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IncreaseInEducationReducer  extends Reducer<Text, IntWritable, Text, IntWritable> {

	int count = 1;
	public void reduce(Text value, Iterable<IntWritable> key, Context context) throws IOException, InterruptedException{

		String line = value.toString();
		
		
		for(String word: line.split("[\\r\\n]+")){
			if(word.length() > 0){
				if((word.indexOf("United States") != -1)
						&&(word.indexOf("graduation")!=-1)
						&&(word.indexOf("tertiary")!=-1)
						&&(word.indexOf("female")!=-1)){
					
					String[] tokens = word.split(",");

							tokens[46].trim();
							tokens[58].trim();
							if((isDouble(tokens[46]))&&(isDouble(tokens[58]))){
								double educationRateOlder = Double.parseDouble(tokens[46]);
								double educationRateNewer = Double.parseDouble(tokens[58]);
								Double educationNew = educationRateNewer - educationRateOlder; 
								String s = tokens[0] + " education rate change from 2000 to 2012: " + educationNew.toString();
									context.write(new Text(s), new IntWritable(count++));
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