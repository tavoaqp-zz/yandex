package org.yandex.estimate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.cli2.builder.ArgumentBuilder;
import org.apache.commons.cli2.builder.DefaultOptionBuilder;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HRegionPartitioner;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.common.AbstractJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yandex.estimate.FreqCounter1.Mapper1;
import org.yandex.estimate.FreqCounter1.Reducer1;

public class YandexRelevanceDriver extends AbstractJob {
	
	private static final Logger log = LoggerFactory.getLogger(YandexRelevanceDriver.class);
	public static String BINARY_OUTPUT="binary";
	public static String BINARY_OUTPUT_KEY="binary";
	public static String PARSE_OPTION="parse";
	public static String PARSE_OPTION_KEY="parse";
	public static String MAXIMUM_EM_RUNS="emRuns";
	public static String DUMP_TO_CLICK_EVENT="false";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new YandexRelevanceDriver(), args);		
	}
	
	public static DefaultOptionBuilder binarySessionsOutOption() {
	    return new DefaultOptionBuilder()
	        .withLongName(BINARY_OUTPUT)
	        .withRequired(true)
	        .withArgument(
	            new ArgumentBuilder().withName(BINARY_OUTPUT).withMinimum(1)
	                .withMaximum(1).create())
	        .withDescription(
	            "The path where job will store binary search sessions")
	        .withShortName("-bo");
	  }
	public static DefaultOptionBuilder justParseOption() {
	    return new DefaultOptionBuilder()
	        .withLongName(PARSE_OPTION)
	        .withRequired(true)
	        .withArgument(
	            new ArgumentBuilder().withName(PARSE_OPTION).withMinimum(1)
	                .withMaximum(1).create())
	        .withDescription(
	            "The path where job will store binary search sessions")
	        .withShortName("-bo");
	  }	
	public static DefaultOptionBuilder maximumEMRunsOption() {
	    return new DefaultOptionBuilder()
	        .withLongName(MAXIMUM_EM_RUNS)
	        .withRequired(true)
	        .withArgument(
	            new ArgumentBuilder().withName(MAXIMUM_EM_RUNS).withMinimum(1)
	                .withMaximum(1).create())
	        .withDescription(
	            "Maximum loops for EM algorithm")
	        .withShortName("-bo");
	  }
	
	@Override
	public int run(String[] arg0) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(justParseOption().create());
		maximumEMRunsOption();
		if (parseArguments(arg0) == null) {
		      return -1;
		    }
		Path input = getInputPath();
		Path output = getOutputPath();
		String parseFileVal=getOption(PARSE_OPTION);
		boolean parseFile=false;
		if (StringUtils.isNotBlank(parseFileVal))
			parseFile=Boolean.parseBoolean(parseFileVal);
		if (getConf() == null) {
			setConf(new Configuration());
		}
		run(input,output,parseFile);
		return 0;
	}
	
	private void run(Path input, Path output, boolean parseFile) throws Exception {
		log.info("Starting YANDEX RELEVANCE!");
		Configuration conf=getConf();
		Job job = new Job(getConf(), "Parsing log file and putting observations to click_event");
		if (input==null)
			throw new RuntimeException("input is null");
		log.info("Starting job to insert clicks in table!");
		job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(DocObservations.class);
	    job.setMapperClass(UrlDocMapper.class);
	    job.setCombinerClass(UrlDocCombiner.class);
	    TableMapReduceUtil.initTableReducerJob("click_event", UrlDocReducer.class, job);
	    
	    FileInputFormat.addInputPath(job, input);
	    
	    job.setInputFormatClass(SequenceFileInputFormat.class);
	    
	    job.waitForCompletion(true);
	    
	    Job job1 = new Job(getConf(), "Parsing log file and putting observations to binary_sessions");
		if (input==null)
			throw new RuntimeException("input is null");
		log.info("Starting job1 to insert sessions in table!");
		job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(DocObservations.class);
	    job1.setMapperClass(UrlDocMapper.class);
	    job1.setCombinerClass(UrlDocCombiner.class);
	    job1.setReducerClass(UrlBinarySessionReducer.class);
	    TableMapReduceUtil.initTableReducerJob("binary_sessions", UrlBinarySessionReducer.class, job1);    
	    
	    FileInputFormat.addInputPath(job1, input);
	    
	    job1.setInputFormatClass(SequenceFileInputFormat.class);
	    
	    job1.waitForCompletion(true);
	    
	 
	    
		
	    
	}
	
}
