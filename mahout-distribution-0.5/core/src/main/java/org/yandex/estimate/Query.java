package org.yandex.estimate;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

public class Query implements Writable{

	private DocObservations docs;
	private IntWritable id;
	private IntWritable session_id;
	private IntWritable region;
	private IntWritable time;
	
	public Query() {
		super();
		docs=new DocObservations();
		id=new IntWritable();
		session_id=new IntWritable();
		region=new IntWritable();
		time=new IntWritable();
	}
	
	public void fillData(DocObservations obs)
	{
		this.docs.set(obs.get());
		if (this.docs.get().length>0)
		{
			DocEvent firstDoc=(DocEvent)this.docs.get()[0];
			id.set(firstDoc.getQuery_id());
			session_id.set(firstDoc.getSession_id());
			region.set(firstDoc.getRegion());
			time.set(obs.getMainTime());
		}
		else
			throw new RuntimeException("Lista vazia!");
	}

	@Override
	public void readFields(DataInput arg0) throws IOException {
		id.readFields(arg0);
		session_id.readFields(arg0);
		region.readFields(arg0);
		time.readFields(arg0);
		docs.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		id.write(arg0);
		session_id.write(arg0);
		region.write(arg0);
		time.write(arg0);
		docs.write(arg0);	
	}

	public DocObservations getDocs() {		
		return docs;
	}
//	
//	public void setDocs(DocEvent[] docs)
//	{
//		this.docs.set(docs);
//	}
	
	public String toString(){
		return "[Main time: "+this.time.get()+" "+docs.toString()+"]";
	}
	public boolean hasDoc(DocEvent event)
	{
		Writable[] docs=this.docs.get();
		for (Writable doc:docs)
		{
			if (((DocEvent)doc).getId()==event.getId())
				return true;
		}
		return false;
	}
	
	public void updateDoc(DocEvent event)
	{
		Writable[] docs=this.docs.get();
		for (Writable doc:docs)
		{
			if (((DocEvent)doc).getId()==event.getId())
			{
				((DocEvent)doc).setTime(event.getTime());
				((DocEvent)doc).setClicked(event.getClicked());
			}
		}
	}
	
	public void addDoc(DocEvent event)
	{
		if (this.docs.get()==null)
		{
			this.docs.set(new Writable[] {event});
		}
		else
		{
			List<Writable> currentDocs=new ArrayList<Writable>();
			currentDocs.addAll(Arrays.asList(this.docs.get()));
			currentDocs.add(event);
			this.docs.set(currentDocs.toArray(new DocEvent[0]));
		}
		
	}

	public int getId() {
		return id.get();
	}

	public void setId(int id) {
		this.id.set(id);
	}

	public int getSession_id() {
		return session_id.get();
	}

	public void setSession_id(int session_id) {
		this.session_id.set(session_id);
	}

	public int getRegion() {
		return region.get();
	}

	public void setRegion(int region) {
		this.region.set(region);
	}

	public void calcLastClicked() {
		Writable[] currentDocs=this.docs.get();
		int lastclick=0;
		Arrays.sort(currentDocs, new Comparator<Writable>(){

			@Override
			public int compare(Writable arg0, Writable arg1) {
				DocEvent d1=(DocEvent)arg0;
				DocEvent d2=(DocEvent)arg1;
				return d1.getPosition()-d2.getPosition();
			}
			
		});
		for (int i=0;i<currentDocs.length;i++)
		{
			((DocEvent)currentDocs[i]).setClick_distance(-1);
		}
		
		boolean first=false;
		for (int i = 0; i < currentDocs.length; i++) {			
			if (!first && ((DocEvent) currentDocs[i]).getClicked())
			{
				first=true;
				((DocEvent) currentDocs[i]).setClick_distance(0);
			}
			else{
				if (first){
					int prevDist=((DocEvent) currentDocs[i-1]).getClick_distance();
					if (((DocEvent) currentDocs[i-1]).getClicked()) {					
						((DocEvent) currentDocs[i]).setClick_distance(1);									
					} else {				
						if (prevDist>=0)
							((DocEvent) currentDocs[i]).setClick_distance(prevDist+1);
					}	
				}
			}
		}	
		
		this.docs.set(currentDocs);
		
	}
	
	public static void main(String args[])
	{
		Query query=new Query();
		DocEvent[] docs=new DocEvent[10];
		for (int i=0;i<10;i++)
		{
			DocEvent event=new DocEvent();
			if (i==1 || i==3)
			{
				
				event.setClicked(true);				
			}
			else
				event.setClicked(false);
			event.setPosition(i+1);
			docs[i]=event;			
		}
		query.getDocs().set(docs);
		
		query.calcLastClicked();
		System.out.println(query.toString());
	}

	public int getTime() {
		return time.get();
	}

	public void setTime(int time) {
		this.time.set(time);
	}

}
