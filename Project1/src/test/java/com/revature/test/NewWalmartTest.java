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

import com.revature.map.IndustrialEmploymentMapper;
import com.revature.reduce.NewWalmartReducer;

public class NewWalmartTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		IndustrialEmploymentMapper mapper = new IndustrialEmploymentMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
	    NewWalmartReducer reducer = new NewWalmartReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
		
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment in industry"
				+ ", 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.8417788946383\""
				+ ",\"75.7451372747231\",\"75.428706359347\",\"75.1743877888956\",\"75.063150694736\""
				+ ",\"74.8100333327059\",\"74.5560073268663\",\"74.1962421996127\",\"74.0954708277255\""
				+ ",\"73.9489970024712\",\"73.7461080032518\",\"73.2774040912997\",\"73.0718231150595\","
				+ "\"73.0531516823621\",\"73.0941332970239\",\"73.0576439054338\",\"73.1193907760343\""
				+ ",\"72.8379994624721\",\"72.1339009843366\",\"72.0846370027268\",\"72.0140632798919\""
				+ ",\"71.9899178190254\",\"71.9234108185349\",\"71.948524392694\",\"72.0275542304481\""
				+ ",\"72.0073062034121\""));
		mapDriver.withOutput(new Text("United States;USA;Employment in industry, 15+, male (%) "
				+ "(modeled ILO estimate);SL.EMP.TOTL.SP.MA.ZS;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;"
				+ "75.8417788946383;75.7451372747231;75.428706359347;75.1743877888956;75.063150694736;"
				+ "74.8100333327059;74.5560073268663;74.1962421996127;74.0954708277255;73.9489970024712;"
				+ "73.7461080032518;73.2774040912997;73.0718231150595;73.0531516823621;73.0941332970239;"
				+ "73.0576439054338;73.1193907760343;72.8379994624721;72.1339009843366;72.0846370027268;"
				+ "72.0140632798919;71.9899178190254;71.9234108185349;71.948524392694;72.0275542304481;"
				+ "72.0073062034121"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("United States;USA!;Employment in industry, 15+, male (%) "
				+ "(modeled ILO estimate);SL.EMP.TOTL.SP.MA.ZS;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;"
				+ "75.8417788946383;75.7451372747231;75.428706359347;75.1743877888956;75.063150694736;"
				+ "74.8100333327059;74.5560073268663;74.1962421996127;74.0954708277255;73.9489970024712;"
				+ "73.7461080032518;73.2774040912997;73.0718231150595;73.0531516823621;73.0941332970239;"
				+ "73.0576439054338;73.1193907760343;72.8379994624721;72.1339009843366;72.0846370027268;"
				+ "60;63;64;68;75"), values);
		reduceDriver.withOutput(new Text("POSSIBLE WALMART LOCATION!   United States 2014 to 2015") , new DoubleWritable(7.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"United States\",\"USA\",\"Employment in industry"
				+ ", 15+, male (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.MA.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"75.8417788946383\""
				+ ",\"75.7451372747231\",\"75.428706359347\",\"75.1743877888956\",\"75.063150694736\""
				+ ",\"74.8100333327059\",\"74.5560073268663\",\"74.1962421996127\",\"74.0954708277255\""
				+ ",\"73.9489970024712\",\"73.7461080032518\",\"73.2774040912997\",\"73.0718231150595\","
				+ "\"73.0531516823621\",\"73.0941332970239\",\"73.0576439054338\",\"73.1193907760343\""
				+ ",\"72.8379994624721\",\"72.1339009843366\",\"72.0846370027268\",\"60\""
				+ ",\"63\",\"64\",\"68\",\"75\""));
		mapReduceDriver.withOutput(new Text("POSSIBLE WALMART LOCATION!   United States 2014 to 2015"), new DoubleWritable(7.0));
		mapReduceDriver.runTest();
	}
	
}
