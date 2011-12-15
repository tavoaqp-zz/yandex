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
		TableReducer<Text, Session, ImmutableBytesWritable> {

	private boolean dumpToClickEvent;

	public void reduce(Text key, Iterable<Session> values, Context context)
			throws IOException, InterruptedException {
		Iterator<Session> obsIt = values.iterator();
		while (obsIt.hasNext()) {
			Session session = obsIt.next();
			if (dumpToClickEvent) {
				// log.info("Writing session to database");
				for (Writable query : session.getQueries().get()) {
					// log.info(((Query)query).toString());
					Query queryObj = (Query) query;
					for (Writable doc : queryObj.getDocs().get()) {
						DocEvent docEvent = (DocEvent) doc;

						String id = session.getId() + "-" + queryObj.getId()
								+ "-" + queryObj.getTime() + "-"
								+ docEvent.getId();
						ImmutableBytesWritable docKey = new ImmutableBytesWritable(
								Bytes.toBytes(id));
						Put put = new Put(docKey.get());
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("doc_id"),
								Bytes.toBytes(docEvent.getId()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("query_id"),
								Bytes.toBytes(docEvent.getQuery_id()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("session_id"),
								Bytes.toBytes(docEvent.getSession_id()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("time"),
								Bytes.toBytes(docEvent.getTime()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("distance"),
								Bytes.toBytes(docEvent.getClick_distance()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("position"),
								Bytes.toBytes(docEvent.getPosition()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("region"),
								Bytes.toBytes(docEvent.getRegion()));
						put.add(Bytes.toBytes("details"),
								Bytes.toBytes("clicked"),
								Bytes.toBytes(docEvent.getClicked()));
						context.write(docKey, put);
					}
				}
			} else {
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
				ImmutableBytesWritable sessionKey = new ImmutableBytesWritable(
						Bytes.toBytes(key.toString()));
				context.write(sessionKey, binaryPut);
			}
		}

	}

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();
		dumpToClickEvent = Boolean.parseBoolean(conf
				.get(ParseDataToBinary.DUMP_TO_CLICK_EVENT));
	}
}
