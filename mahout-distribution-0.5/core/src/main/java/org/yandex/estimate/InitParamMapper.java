package org.yandex.estimate;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.IntWritable;

public class InitParamMapper extends TableMapper<ImmutableBytesWritable, IntWritable>{

	@Override
	protected void map(ImmutableBytesWritable key, Result value,
			org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		super.map(key, value, context);
	}
	
	

}
