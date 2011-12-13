package org.yandex.estimate;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class QueryObservations extends ArrayWritable{

	public QueryObservations() {
		super(Query.class);
	}

	public void addQueries(ArrayList<Query> queries)
	{
		if (this.get()==null)
			this.set(queries.toArray(new Query[0]));
		else
		{
			Query[] oldQueries=(Query[]) this.get();
			ArrayList<Query> newQueries=new ArrayList<Query>();
			newQueries.addAll(Arrays.asList(oldQueries));
			newQueries.addAll(queries);
			this.set(newQueries.toArray(new Query[0]));			
		}
	}
}

