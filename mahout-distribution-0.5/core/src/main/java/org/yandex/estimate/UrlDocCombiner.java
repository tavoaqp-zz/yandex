package org.yandex.estimate;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlDocCombiner extends
		Reducer<Text, DocObservations, Text, DocObservations> {

	private final Logger log = LoggerFactory.getLogger(UrlDocCombiner.class);

	@Override
	public void reduce(Text key, Iterable<DocObservations> val, Context ctx)
			throws IOException, InterruptedException {
//		log.info("Processing session " + key.toString());
		
		Iterator<DocObservations> it = val.iterator();
		ArrayList<DocEvent> newObs=new ArrayList<DocEvent>();
		while (it.hasNext())
		{
			Writable[] docs=it.next().get();
			for (Writable doc:docs)
			{
				newObs.add((DocEvent) doc);
			}			
		}
		DocObservations newObsToEmit=new DocObservations();
		newObsToEmit.set(newObs.toArray(new DocEvent[0]));
		ctx.write(key, newObsToEmit);
//		
//		
//		HashMap<String, DocObservations> queries = new HashMap<String, DocObservations>();
//		ArrayList<DocEvent> clicked = new ArrayList<DocEvent>();
//		int count = 0;
//		while (it.hasNext()) {
//			DocObservations docEventList = it.next();
//			DocEvent docEvent = (DocEvent) docEventList.get()[0];
//			if (docEvent.getClicked()) {
//				clicked.add(docEvent);
//			} else {
//				String obsKey=docEvent.getQuery_id()+"-"+docEvent.getTime();
//				if (queries.containsKey(obsKey)) {
//					DocObservations currList = queries.get(obsKey);				
//					int newSize = currList.get().length + 1;
//					DocEvent[] newList = new DocEvent[newSize];
//					System.arraycopy(currList.get(), 0, newList, 0, currList.get().length);
//					newList[newSize - 1] = docEvent;
//					currList.set(newList);					
//				} else {
//					DocObservations obs=new DocObservations();
//					obs.setMainTime(docEvent.getTime());
//					obs.set(new DocEvent[] { docEvent });
//					queries.put(obsKey, obs);
//				}
//			}
//			count++;
//		}
//		
//
//		log.info("Session " + key.toString() + " has " + count + " docs and "+queries.size()+" queries");
//		ArrayList<Object> fullSeq=new ArrayList<Object>();
//		fullSeq.addAll(queries.values());
//		fullSeq.addAll(clicked);
//		
//		//a more efficient algorithm would first merge the two collections
//		//instead of sorting them afterwards
//		Collections.sort(fullSeq, new Comparator<Object>(){
//			@Override
//			public int compare(Object o1, Object o2) {
//				int t1=0;
//				int t2=0;
//				if (o1 instanceof DocObservations)
//					t1=((DocObservations)o1).getMainTime();
//				else if (o1 instanceof DocEvent)
//					t1=((DocEvent)o1).getTime();
//				
//				if (o2 instanceof DocObservations)
//					t2=((DocObservations)o2).getMainTime();
//				else if (o2 instanceof DocEvent)
//					t2=((DocEvent)o2).getTime();
//				
//				return t1-t2;
//			}			
//		});
//		
//		
//		int lastQuery=0;
//		for (int i=0;i<fullSeq.size();i++)
//		{
//			Object obj=fullSeq.get(i);
//			if (obj instanceof DocEvent && i>0)
//			{				
//				Object lastQueryObj=fullSeq.get(lastQuery);
//				((DocObservations)lastQueryObj).updateDoc((DocEvent)obj);	
//			}
//			else
//			{
//				if (obj instanceof DocEvent)
//				{
//					for (int j=0;j<fullSeq.size();j++)
//					{
//						Object obj1=fullSeq.get(j);
//						if (obj1 instanceof DocEvent)
//						{
//							log.error("DocEvent: "+((DocEvent)obj1).getTime());
//						}else
//						{
//							log.error("DocObservations: "+((DocObservations)obj1).getMainTime());
//						}
//					}
//					throw new RuntimeException("Session "+key.toString()+" started with a click?");
//				}
//				else
//					lastQuery=i;
//			}
//		}
//		
//		
//		for (DocObservations query : queries.values()) {			
//			ctx.write(key, query);
//		}

	}
}
