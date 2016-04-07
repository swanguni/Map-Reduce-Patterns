package com.examples.mr.tweeter_keyword_spotting;

import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.Text; 
import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat; 

public class KeywordSpottingDriver { 
	public static void main(String[] args) throws Exception { 
	
		// Initiate configuration
		Configuration configx = new Configuration(); 
	
		// Add resource files 
		configx.addResource(new Path("/etc/hadoop/conf/core-site.xml")); 
		configx.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml")); 
		// Create MapReduce job 
		Job devxmapjob = new Job(configx,"ComplexWCDriver.class"); 
		devxmapjob.setJarByClass(KeywordSpottingDriver.class); 
		devxmapjob.setJobName("DevX MapReduce Job"); 
		
		// Set output kay and value class 
		devxmapjob.setOutputKeyClass(Text.class); 
		devxmapjob.setOutputValueClass(Text.class); 
		
		// Set Map class 
		devxmapjob.setMapperClass(KeywordSpottingMapper.class); 
		// Set Combiner class 
		devxmapjob.setCombinerClass(KeywordSpottingReducer.class); 
		// Set Reducer class 
		devxmapjob.setReducerClass(KeywordSpottingReducer.class); 
		
		// Set Map output key and value classes 
		devxmapjob.setMapOutputKeyClass(Text.class); 
		devxmapjob.setMapOutputValueClass(Text.class); 
		
		// Set number of reducer tasks 
		devxmapjob.setNumReduceTasks(10); 
		// Set input and output format classes 
		devxmapjob.setInputFormatClass(TextInputFormat.class); 
		devxmapjob.setOutputFormatClass(TextOutputFormat.class); 
		
		// Set input and output path 
		FileInputFormat.addInputPath(devxmapjob, new Path("/root/examples/data/input/")); 
		FileOutputFormat.setOutputPath(devxmapjob,new Path("/root/examples/data/output")); 
		// Start MapReduce job 
		devxmapjob.waitForCompletion(true); 
		} 
	} 
 