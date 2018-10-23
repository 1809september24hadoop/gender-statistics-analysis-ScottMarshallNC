package com.revature.test;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.revature.IncreaseInEducation;
import com.revature.map.FemaleGraduationMapper;
import com.revature.reduce.WorldFemaleEmploymentReducer;
import com.revature.reduce.WorldMaleEmploymentReducer;
import com.revature.reduce.FemaleGraduationReducer;

public class MapInOneLineTest {

//	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
//	private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
//	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;
//	
//	@Before
//	public void setUp(){
//		
//		GenderStatMapper mapper = new GenderStatMapper();
//		mapDriver = new MapDriver<>();
//		mapDriver.setMapper(mapper);
//		
//		SumReducer reducer = new SumReducer();
//		reduceDriver = new ReduceDriver<>();
//		reduceDriver.setReducer(reducer);
//		
//		IncreaseInEducation reducerForEducationRate = new IncreaseInEducation();
//		reduceDriver = new ReduceDriver<>();
//		reduceDriver.setReducer(reducer);
//		
//		PercentMaleEmployementByCountryReducer reducerForMaleEmployement = new PercentMaleEmployementByCountryReducer();
//		reduceDriver = new ReduceDriver<>();
//		reduceDriver.setReducer(reducer);
//		
//		PercentFemaleEmployementByCountryReducer reducerForFemaleEmployement = new PercentFemaleEmployementByCountryReducer();
//		reduceDriver = new ReduceDriver<>();
//		reduceDriver.setReducer(reducer);
//		
//	}
//	
//	@Test
//	public void testMapper(){
//		mapDriver.withInput(new LongWritable(1), new Text());
//		
//		mapDriver.withOutput(new Text(""), new IntWritable(1));
//	
//		mapDriver.runTest();
//	}
//	
//	@Test
//	public void testReducer(){
//		List<IntWritable> values = new ArrayList<IntWritable>();
//		values.add(new IntWritable(1));
//		values.add(new IntWritable(1));
//		values.add(new IntWritable(1));
//		
//		reduceDriver.withInput(new Text("cat"), values);
//		reduceDriver.withOutput(new Text("cat"), new IntWritable(3));
//	}
//	
//	@Test
//	public void testMapReduce() {
//		mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
//		
//		mapReduceDriver.addOutput(new Text("cat"), new IntWritable(2));
//		mapReduceDriver.addOutput(new Text("dog"), new IntWritable(1));
//		mapReduceDriver.runTest();
//	}
	
}
