package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.map.IncreaseInEducationMapper;
import com.revature.reduce.IncreaseInEducationReducer;

public class IncreaseInEducationTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		IncreaseInEducationMapper mapper = new IncreaseInEducationMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		IncreaseInEducationReducer reducer = new IncreaseInEducationReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
		
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), new Text("United States\",\",\"completed Education,"
				+ " tertiary, female\",\",\",\",\",\",\",\",\",\","
				+ "\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\","
				+ "\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",7.0\",8.0"));
		mapDriver.withOutput(new Text("United States;;completed Education, tertiary, female;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;7.0;8.0"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("United States;;completed Education, tertiary, female;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;7.0;8.0"), values);
		reduceDriver.withOutput(new Text("United States completed Education, tertiary, female 2005 to 2006"), new DoubleWritable(1.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("United States\",\",\"completed Education,"
				+ " tertiary, female\",\",\",\",\",\",\",\",\",\","
				+ "\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\","
				+ "\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",\",7.0\",8.0"));
		mapReduceDriver.withOutput(new Text("United States completed Education, tertiary, female 2005 to 2006"), new DoubleWritable(1.0));
		mapReduceDriver.runTest();
	}
	
}
