package org.yandex.estimate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.IntWritable;

public class DocObservations extends ArrayWritable {

	private IntWritable mainTime;
	
	public DocObservations() {
		super(DocEvent.class);
		mainTime=new IntWritable();
	}
	
	public String toString(){
		String val="{Main time: "+mainTime.get();
		for (Writable doc:this.get())
		{
			val+=((DocEvent)doc).toString()+"\n";
		}
		val+="}";
		return val;
	}
	
	public boolean hasDoc(DocEvent event)
	{
		DocEvent[] docs=(DocEvent[]) get();
		
		for (DocEvent doc:docs)
		{
			if (doc.getId()==event.getId())
				return true;
		}
		return false;
	}
	
	public void updateDoc(DocEvent event)
	{
		DocEvent[] docs=(DocEvent[]) get();
		for (DocEvent doc:docs)
		{
			if (doc.getId()==event.getId())
			{
				doc.setTime(event.getTime());
				doc.setClicked(event.getClicked());
			}
		}
	}

	public int getMainTime() {
		return mainTime.get();
	}

	public void setMainTime(int mainTime) {
		this.mainTime.set(mainTime);
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		mainTime.readFields(arg0);
		super.readFields(arg0);		
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		mainTime.write(arg0);
		super.write(arg0);
	}
}


