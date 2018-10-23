package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.WorldFemaleEmploymentMapper;
import com.revature.reduce.WorldFemaleEmploymentReducer;

public class WorldFemaleEmployement {

	public static void main(String[] args) throws Exception {
		if(args.length != 2){
			System.out.printf("Usage: TestCheck <input dir> <output dir>\n");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		job.setJarByClass(WorldFemaleEmploymentMapper.class);
		job.setJobName("Test 3");
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WorldFemaleEmploymentMapper.class);
		job.setReducerClass(WorldFemaleEmploymentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
	}
}