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

public class ParseDataToBinary extends AbstractJob {
	
	private static final Logger log = LoggerFactory.getLogger(ParseDataToBinary.class);
	public static String BINARY_OUTPUT="binary";
	public static String BINARY_OUTPUT_KEY="binary";
	
	public static String PARSE_OPTION="parse";
	public static String PARSE_OPTION_KEY="parse";
	
	public static String MAXIMUM_EM_RUNS="emRuns";
	
	public static String DUMP_TO_CLICK_EVENT="false";
	
	public static void main(String[] args) throws Exception {
		ToolRunner.run(new Configuration(), new ParseDataToBinary(), args);		
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
	
	@Override
	public int run(String[] arg0) throws Exception {
		addInputOption();
		addOutputOption();
		addOption(justParseOption().create());
		
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
		if (parseFile)
		{
			Job job = new Job(conf, "Parsing log file and putting sessions to disk");
			if (input==null)
				throw new RuntimeException("input is null");
			log.info("Starting job to insert clicks in table!");
			job.setMapOutputKeyClass(Text.class);
		    job.setMapOutputValueClass(DocObservations.class);
		    job.setMapperClass(UrlDocMapper.class);
		    job.setCombinerClass(UrlDocCombiner.class);
		    job.setReducerClass(UrlBinaryFileSessionReducer.class);    
		    
		    FileInputFormat.addInputPath(job, input);
		    FileOutputFormat.setOutputPath(job, output);
		    
		    job.setInputFormatClass(SequenceFileInputFormat.class);
		    job.setOutputFormatClass(SequenceFileOutputFormat.class);
		    
		    job.waitForCompletion(true);
		}
		conf.set(DUMP_TO_CLICK_EVENT,"true");
	    Job job1 = new Job(conf, "Putting files to click_event");
		job1.setMapOutputKeyClass(Text.class);
	    job1.setMapOutputValueClass(Session.class);
	    job1.setMapperClass(Mapper.class);
	    TableMapReduceUtil.initTableReducerJob("click_event", UrlBinarySessionReducer.class, job1);	    
	    FileInputFormat.addInputPath(job1, output);	    
	    job1.setInputFormatClass(SequenceFileInputFormat.class);	    
	    job1.waitForCompletion(true);
	    
	    conf.set(DUMP_TO_CLICK_EVENT,"false");
	    Job job2 = new Job(conf, "Putting files to binary_sessions");
		job2.setMapOutputKeyClass(Text.class);
	    job2.setMapOutputValueClass(Session.class);
	    job2.setMapperClass(Mapper.class);
	    TableMapReduceUtil.initTableReducerJob("click_event", UrlBinarySessionReducer.class, job2);	    
	    FileInputFormat.addInputPath(job2, output);	    
	    job2.setInputFormatClass(SequenceFileInputFormat.class);	    
	    job2.waitForCompletion(true);   
	}
	
}
