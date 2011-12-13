package org.yandex.estimate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Session implements Writable{
	
	private IntWritable id;
	private QueryObservations queries;
	
	public Session(){
		id=new IntWritable();
		queries=new QueryObservations();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		queries.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		queries.readFields(in);		
	}

	public int getId() {
		return id.get();
	}

	public void setId(int id) {
		this.id.set(id);
	}

	public QueryObservations getQueries() {
		return queries;
	}

	public void setQueries(QueryObservations queries) {
		this.queries = queries;
	}

}
