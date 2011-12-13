package org.yandex.estimate;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UrlBinarySessionReducer extends
	TableReducer<Text, DocObservations, ImmutableBytesWritable> {

	private final Logger log = LoggerFactory.getLogger(UrlDocReducer.class);

	public Collection<DocObservations> findQueries(Text key,
			Iterator<DocObservations> it) {
		HashMap<String, DocObservations> queries = new HashMap<String, DocObservations>();
		ArrayList<DocEvent> clicked = new ArrayList<DocEvent>();
		int count = 0;
		while (it.hasNext()) {
			DocObservations docEventList = it.next();
			Writable[] docEvents = docEventList.get();
			for (Writable docEventOrig : docEvents) {
				DocEvent docEvent = (DocEvent) docEventOrig;
				if (docEvent.getClicked()) {
					clicked.add(docEvent);
				} else {
					String obsKey = docEvent.getQuery_id() + "-"
							+ docEvent.getTime();
					if (queries.containsKey(obsKey)) {
						DocObservations currList = queries.get(obsKey);
						int newSize = currList.get().length + 1;
						DocEvent[] newList = new DocEvent[newSize];
						System.arraycopy(currList.get(), 0, newList, 0,
								currList.get().length);
						newList[newSize - 1] = docEvent;
						currList.set(newList);
					} else {
						DocObservations obs = new DocObservations();
						obs.setMainTime(docEvent.getTime());
						obs.set(new DocEvent[] { docEvent });
						queries.put(obsKey, obs);
					}
				}
				count++;
			}

		}

		// log.info("Session " + key.toString() + " has " + count +
		// " docs and "+queries.size()+" queries");
		ArrayList<Object> fullSeq = new ArrayList<Object>();
		fullSeq.addAll(queries.values());
		fullSeq.addAll(clicked);

		// a more efficient algorithm would first merge the two collections
		// instead of sorting them afterwards
		Collections.sort(fullSeq, new Comparator<Object>() {
			@Override
			public int compare(Object o1, Object o2) {
				int t1 = 0;
				int t2 = 0;
				if (o1 instanceof DocObservations)
					t1 = ((DocObservations) o1).getMainTime();
				else if (o1 instanceof DocEvent)
					t1 = ((DocEvent) o1).getTime();

				if (o2 instanceof DocObservations)
					t2 = ((DocObservations) o2).getMainTime();
				else if (o2 instanceof DocEvent)
					t2 = ((DocEvent) o2).getTime();

				return t1 - t2;
			}
		});

		int lastQuery = 0;
		for (int i = 0; i < fullSeq.size(); i++) {
			Object obj = fullSeq.get(i);
			if (obj instanceof DocEvent && i > 0) {
				Object lastQueryObj = fullSeq.get(lastQuery);
				((DocObservations) lastQueryObj).updateDoc((DocEvent) obj);
			} else {
				if (obj instanceof DocEvent) {
					String seq = "";
					for (int j = 0; j < fullSeq.size(); j++) {
						Object obj1 = fullSeq.get(j);
						if (obj1 instanceof DocEvent) {
							seq += "DocEvent: " + ((DocEvent) obj1).getTime()
									+ "\n";
						} else {
							seq += "DocObservations: "
									+ ((DocObservations) obj1).getMainTime()
									+ "\n";
						}
					}
					throw new RuntimeException("Session " + key.toString()
							+ " started with a click?\n" + seq);
				} else
					lastQuery = i;
			}
		}

		return queries.values();
	}

	public void reduce(Text key, Iterable<DocObservations> values,
			Context context) throws IOException, InterruptedException {
		// log.info("Reducing session " + key.toString());
		Iterator<DocObservations> obsIt = values.iterator();
		Collection<DocObservations> obsQueries = findQueries(key, obsIt);
		Iterator<DocObservations> it = obsQueries.iterator();

		Session session = new Session();
		ArrayList<Query> queries = new ArrayList<Query>();
		while (it.hasNext()) {
			DocObservations obs = it.next();
			Query query = new Query();
			query.fillData(obs);
			query.calcLastClicked();
			queries.add(query);
		}
		session.setId(queries.get(0).getSession_id());
		session.getQueries().addQueries(queries);

		// If somehow we could write sessions to disk also!
		Put binaryPut = new Put(key.getBytes());
		ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
		ObjectOutputStream output = new ObjectOutputStream(byteOutput);
		session.write(output);
		binaryPut.add(Bytes.toBytes("details"), Bytes.toBytes("data"),
				byteOutput.toByteArray());
		
		output.close();
		byteOutput.close();
		output = null;
		byteOutput = null;
		ImmutableBytesWritable sessionKey = new ImmutableBytesWritable(Bytes.toBytes(key.toString()));
		context.write(sessionKey, binaryPut);
	}
}
