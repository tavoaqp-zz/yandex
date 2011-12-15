package org.yandex.estimate;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

public class SessionArray extends ArrayWritable{

	public SessionArray() {
		super(Session.class);
	}

}
