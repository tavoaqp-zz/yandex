package org.yandex.estimate;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.IntWritable;

public class InitParamReducer extends TableReducer<ImmutableBytesWritable, Result, ImmutableBytesWritable>{

	@Override
	protected void reduce(ImmutableBytesWritable arg0,
			Iterable arg1,
			org.apache.hadoop.mapreduce.Reducer.Context arg2)
			throws IOException, InterruptedException {

//		super.reduce(arg0, arg1, arg2);
	}

}
