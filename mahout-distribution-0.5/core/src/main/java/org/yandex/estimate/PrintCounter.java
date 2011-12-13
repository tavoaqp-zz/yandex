package org.yandex.estimate;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class PrintCounter {

    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(conf, "click_event");

        Scan scan = new Scan();
        SingleColumnValueFilter filter=new SingleColumnValueFilter(Bytes.toBytes("details"),Bytes.toBytes("session_id"),
        		CompareOp.EQUAL,Bytes.toBytes(0));
        scan.setFilter(filter);
        ResultScanner scanner = htable.getScanner(scan);
        Result r;
        while (((r = scanner.next()) != null)) {
            ImmutableBytesWritable b = r.getBytes();
            byte[] key = r.getRow();
            String userId = Bytes.toString(key);
            byte[] docId = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("doc_id"));
            byte[] queryId = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("query_id"));
            byte[] sessionId = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("session_id"));
            byte[] time = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("time"));
            byte[] distance = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("distance"));
            byte[] position = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("position"));
            byte[] region = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("region"));
            byte[] clicked = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("clicked"));
            System.out.println("key= "+userId+", doc_id: "+Bytes.toInt(docId)+", query_id: "+Bytes.toInt(queryId)+
            		", session_id: "+Bytes.toInt(sessionId)+", time: "+Bytes.toInt(time)+
            		", distance: "+Bytes.toInt(distance)+", position: "+Bytes.toInt(position)+",region: "+Bytes.toInt(region)+
            		", clicked: "+Bytes.toBoolean(clicked));
            
            
//            Put put = new Put(docKey.get());
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("doc_id"), Bytes.toBytes(docEvent.getId()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("query_id"), Bytes.toBytes(docEvent.getQuery_id()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("session_id"), Bytes.toBytes(docEvent.getSession_id()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("time"), Bytes.toBytes(docEvent.getTime()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("distance"), Bytes.toBytes(docEvent.getClick_distance()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("position"), Bytes.toBytes(docEvent.getPosition()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("region"), Bytes.toBytes(docEvent.getRegion()));
//			put.add(Bytes.toBytes("details"), Bytes.toBytes("clicked"), Bytes.toBytes(docEvent.getClicked()));

        }
        scanner.close();
        htable.close();
    }
}