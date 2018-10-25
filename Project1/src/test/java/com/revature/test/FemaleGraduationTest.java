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

import com.revature.map.FemaleGradRateMapper;
import com.revature.reduce.FemaleGradRateReducer;



public class FemaleGraduationTest {

	private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
	private ReduceDriver<Text, IntWritable, Text, DoubleWritable> reduceDriver;
	private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable> mapReduceDriver;
	
	@Before
	public void setUp(){
		
		FemaleGradRateMapper mapper = new FemaleGradRateMapper();
		mapDriver = new MapDriver<>();
		mapDriver.setMapper(mapper);
		
		FemaleGradRateReducer reducer = new FemaleGradRateReducer();
		reduceDriver = new ReduceDriver<>();
		reduceDriver.setReducer(reducer);
		
		mapReduceDriver = new MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, DoubleWritable>();
	    mapReduceDriver.setMapper(mapper);
	    mapReduceDriver.setReducer(reducer);
		
	}
	
	@Test
	public void testMapper(){
		mapDriver.withInput(new LongWritable(1), new Text("\"World\",\"WLD\",\"graduation ratio,"
				+ " tertiary, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"49.0048849081483\""
				+ ",\"49.0952517511666\",\"48.8900926643439\",\"48.8690980408558\",\"48.8425454291542\""
				+ ",\"48.6738302692562\",\"48.5544856145047\",\"48.3131846507726\",\"48.4457130472085\""
				+ ",\"48.4789427159539\",\"48.3973027356075\",\"48.2836643902032\",\"48.2306167028635\""
				+ ",\"48.2850141870354\",\"48.3610634498502\",\"48.1723156782368\",\"48.0797587714126\""
				+ ",\"47.6831054329207\",\"47.1142524546921\",\"46.6975960863054\",\"46.5284920555935\""
				+ ",\"46.4530691098045\",\"46.4442392613018\",\"46.5597634947386\",\"46.5444447857213\""
				+ ",\"46.4441184118518\""));
		mapDriver.withOutput(new Text("World;WLD;graduation ratio, tertiary, female (%) "
				+ "(modeled ILO estimate);SL.EMP.TOTL.SP.FE.ZS;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;"
				+ "49.0048849081483;49.0952517511666;48.8900926643439;48.8690980408558;48.8425454291542;"
				+ "48.6738302692562;48.5544856145047;48.3131846507726;48.4457130472085;48.4789427159539;"
				+ "48.3973027356075;48.2836643902032;48.2306167028635;48.2850141870354;48.3610634498502;"
				+ "48.1723156782368;48.0797587714126;47.6831054329207;47.1142524546921;46.6975960863054;"
				+ "46.5284920555935;46.4530691098045;46.4442392613018;46.5597634947386;46.5444447857213;"
				+ "46.4441184118518"), new IntWritable(1));
		mapDriver.runTest();
	}
	
	@Test
	public void testReducer(){
		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(1));
		
		reduceDriver.withInput(new Text("World;WLD;graduation ratio, tertiary, female (%) "
				+ "(modeled ILO estimate);SL.EMP.TOTL.SP.FE.ZS;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;"
				+ "49.0048849081483;49.0952517511666;48.8900926643439;48.8690980408558;48.8425454291542;"
				+ "48.6738302692562;48.5544856145047;48.3131846507726;48.4457130472085;48.4789427159539;"
				+ "48.3973027356075;48.2836643902032;48.2306167028635;48.2850141870354;48.3610634498502;"
				+ "48.1723156782368;48.0797587714126;47.6831054329207;47.1142524546921;46.6975960863054;"
				+ "46.5284920555935;46.4530691098045;46.4442392613018;46.5597634947386;46.5444447857213;"
				+ "23.0"), values);
		reduceDriver.withOutput(new Text("World most recent female tertiary graduation rate was in 2016: "), new DoubleWritable(23.0));
		reduceDriver.runTest();
	}
	
	@Test
	public void testMapReduce() {
		mapReduceDriver.withInput(new LongWritable(1), new Text("\"World\",\"WLD\",\"graduation ratio,"
				+ " tertiary, female (%) (modeled ILO estimate)\",\"SL.EMP.TOTL.SP.FE.ZS\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\","
				+ "\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"\",\"49.0048849081483\""
				+ ",\"49.0952517511666\",\"48.8900926643439\",\"48.8690980408558\",\"48.8425454291542\""
				+ ",\"48.6738302692562\",\"48.5544856145047\",\"48.3131846507726\",\"48.4457130472085\""
				+ ",\"48.4789427159539\",\"48.3973027356075\",\"48.2836643902032\",\"48.2306167028635\""
				+ ",\"48.2850141870354\",\"48.3610634498502\",\"48.1723156782368\",\"48.0797587714126\""
				+ ",\"47.6831054329207\",\"47.1142524546921\",\"46.6975960863054\",\"46.5284920555935\""
				+ ",\"46.4530691098045\",\"46.4442392613018\",\"46.5597634947386\",\"46.5444447857213\""
				+ ",\"23.0\""));
		mapReduceDriver.withOutput(new Text("World most recent female tertiary graduation rate was in 2016: "), new DoubleWritable(23.0));
		mapReduceDriver.runTest();
	}
	
}